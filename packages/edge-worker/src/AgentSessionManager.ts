import { EventEmitter } from "node:events";
import type {
	APIAssistantMessage,
	APIUserMessage,
	SDKAssistantMessage,
	SDKMessage,
	SDKRateLimitEvent,
	SDKResultMessage,
	SDKStatusMessage,
	SDKSystemMessage,
	SDKUserMessage,
} from "cyrus-claude-runner";
import {
	type AgentPendingWork,
	AgentSessionStatus,
	AgentSessionType,
	CYRUS_EVENTS,
	type CyrusAgentSession,
	type CyrusAgentSessionEntry,
	createLogger,
	cyrusAttributes,
	type IAgentRunner,
	type ILogger,
	type IssueMinimal,
	type LogEventAttributes,
	type RepositoryContext,
	type RunnerType,
	type SerializedCyrusAgentSession,
	type SerializedCyrusAgentSessionEntry,
	type SessionCreator,
	type Workspace,
} from "cyrus-core";
import type { RunnerHealthSnapshot } from "cyrus-opencode-runner";

import {
	formatPendingWorkSummary,
	formatPendingWorkThought,
	formatScheduleWakeupResponse,
	tryParseScheduleWakeupInput,
} from "./PendingWorkFormatter.js";
import {
	type OptionalPluginTelemetrySignal,
	type RunnerHealthSignal,
	SessionObservabilityProjection,
} from "./SessionObservabilityProjection.js";
import type {
	ActivityPostOptions,
	ActivitySignal,
	IActivitySink,
} from "./sinks/index.js";

const OPENCODE_HEARTBEAT_INTERVAL_MS = 60_000;
const OPENCODE_CHECKPOINT_TTL_MS = 15_000;

/**
 * Events emitted by AgentSessionManager
 */
export type AgentSessionManagerEvents = {
	/**
	 * Emitted when a session reaches a terminal state (complete/error/stopped).
	 * Router platform mode listens for this to release the router-side issue
	 * lock + session affinity via RouterConnection.sendSessionState.
	 */
	sessionTerminal: (
		sessionId: string,
		state: "complete" | "error" | "stopped",
	) => void;
	/**
	 * Emitted when a session picks up a runner and starts another turn, i.e. it
	 * has advanced past any terminal state it previously reported. Router platform
	 * mode listens for this to drop the now-stale terminal frame from
	 * RouterConnection's durable replay buffer — the counterpart to
	 * `sessionTerminal`, and what keeps terminal reporting monotonic.
	 */
	sessionResumed: (sessionId: string) => void;
	/**
	 * Emitted when a session blocks on a user answer. Router platform mode
	 * listens for this to report a non-terminal `waiting` state.
	 *
	 * The RUN is waiting whether or not anything is still in flight — that is
	 * the fact this event carries, and it is reported unconditionally so a
	 * session blocked with a live background build is observable rather than
	 * silent. Whether its EXECUTOR may be suspended is a separate question the
	 * listener answers from {@link AgentSessionManager.hasPendingWork}: releasing
	 * affinity is what lets the container be idle-suspended, and suspending one
	 * with a build in flight freezes it. The issue lock is retained either way,
	 * since the session is paused rather than finished.
	 */
	sessionParked: (sessionId: string) => void;
	/**
	 * Emitted when a waiting session stops waiting — answered, cancelled, or
	 * aborted. Router platform mode listens for this to drop any still-unacked
	 * wait frame, so a later reconnect cannot replay it over a live turn.
	 * The counterpart to `sessionParked`, exactly as `sessionResumed` is to
	 * `sessionTerminal`.
	 */
	sessionUnparked: (sessionId: string) => void;
	/**
	 * Emitted when a turn ends but the session is held open by pending work — a
	 * scheduled wakeup, a cron, or a live background task.
	 *
	 * The run is ACTIVE, not waiting and not failed, and this is what makes that
	 * legible: without it a session held open for seven hours by a cron produces
	 * no frame at all, and the router's last word on it is whatever it was doing
	 * before the turn ended.
	 */
	sessionPendingWork: (sessionId: string, pendingWorkCount: number) => void;
};

/**
 * Type-safe event emitter interface for AgentSessionManager
 */
export declare interface AgentSessionManager {
	on<K extends keyof AgentSessionManagerEvents>(
		event: K,
		listener: AgentSessionManagerEvents[K],
	): this;
	emit<K extends keyof AgentSessionManagerEvents>(
		event: K,
		...args: Parameters<AgentSessionManagerEvents[K]>
	): boolean;
}

/**
 * Whether a runner session id may be stored for a later turn to resume.
 *
 * A runner's terminal messages always carry SOME session id — a runner that
 * died before its agent ever started still synthesizes one so the timeline is
 * not left blank — and storing that id is how a single failed start becomes a
 * permanently broken Linear session: every subsequent turn resumes an id the
 * provider never issued and fails on it, including the turn where the user has
 * just fixed the original problem.
 *
 * Runners that can tell the difference say so (`CodexRunner`, where the window
 * is real: its credential preflight fails before the backend is opened).
 * Runners that do not are trusted, which preserves existing behaviour exactly —
 * this narrows a failure mode rather than adding a new gate.
 */
function isResumableRunnerSessionId(runner: IAgentRunner | undefined): boolean {
	if (!runner) return true;
	const probe = (runner as { hasEstablishedRunnerSession?: () => boolean })
		.hasEstablishedRunnerSession;
	return typeof probe === "function" ? probe.call(runner) : true;
}

/**
 * Which agent CLI a runner is, by constructor name.
 *
 * Constructor names rather than a declared field because that is what every
 * existing call site here already keys on — this consolidates three identical
 * ternary chains rather than introducing a fourth. Claude is the fallback for
 * the same reason it always was: it is the default runner, and an unrecognised
 * constructor is far more likely to be a Claude subclass than a new provider
 * that forgot to announce itself.
 */
function runnerTypeOf(runner: IAgentRunner): RunnerType {
	switch (runner.constructor.name) {
		case "GeminiRunner":
			return "gemini";
		case "CodexRunner":
			return "codex";
		case "CursorRunner":
			return "cursor";
		case "OpenCodeRunner":
			return "opencode";
		default:
			return "claude";
	}
}

/**
 * Manages Agent Sessions integration with Claude Code SDK
 * Transforms Claude streaming messages into Agent Session format
 * Handles session lifecycle: create → active → complete/error
 *
 * Single instance shared across all repositories. Activity sinks are
 * registered per-session so each session posts to the correct tracker.
 */
export class AgentSessionManager extends EventEmitter {
	private logger: ILogger;
	private activitySinks: Map<string, IActivitySink> = new Map(); // Per-session activity sinks
	private sessions: Map<string, CyrusAgentSession> = new Map();
	private entries: Map<string, CyrusAgentSessionEntry[]> = new Map(); // Stores a list of session entries per each session by its id
	private activeTasksBySession: Map<string, string> = new Map(); // Maps session ID to active Task tool use ID
	private toolCallsByToolUseId: Map<string, { name: string; input: any }> =
		new Map(); // Track tool calls by their tool_use_id
	private lastAssistantBodyBySession: Map<string, string> = new Map(); // Buffer: last assistant text per session for posting as response on result
	private lastAssistantBodyIsToolInputBySession: Map<string, boolean> =
		new Map(); // Whether the buffered body above is a tool_use input JSON (no trailing assistant text) — guards against posting raw JSON as the "response" (CYPACK-1177)
	private bufferedAssistantEntryBySession: Map<string, CyrusAgentSessionEntry> =
		new Map(); // One-behind buffer: holds last assistant entry until next message or result
	private taskSubjectsByToolUseId: Map<string, string> = new Map(); // Cache TaskCreate subjects by toolUseId until result arrives with task ID
	private taskSubjectsById: Map<string, string> = new Map(); // Cache task subjects by task ID (e.g., "1" → "Fix login bug")
	private activeStatusActivitiesBySession: Map<string, string> = new Map(); // Maps session ID to active compacting status activity ID
	private stopRequestedSessions: Set<string> = new Set(); // Sessions explicitly stopped by user signal
	/**
	 * Sessions for which "sessionTerminal" has already been emitted. A session
	 * can reach a terminal state either by the SDK yielding a result
	 * ({@link completeSession}) or by being killed outright
	 * ({@link abortSession}); this keeps the observer notified exactly once even
	 * if both happen.
	 */
	private terminalEmittedSessions: Set<string> = new Set();
	/**
	 * Flushes this manager's state to durable storage. Injected by the
	 * EdgeWorker (see {@link setPersistStateHook}) and awaited immediately
	 * before "sessionTerminal" is emitted, so the terminal status and the final
	 * response entry are on disk *before* the observers act on them.
	 */
	private persistState?: () => Promise<void>;
	// Per-session serialization queue for handleClaudeMessage. The EdgeWorker's
	// onMessage callback is fire-and-forget, so without serialization the async
	// handlers can interleave — causing tool_result to be processed before its
	// matching tool_use registers in toolCallsByToolUseId (seen with parallel
	// deferred tools like ToolSearch, where a tool_use and its tool_result can
	// arrive back-to-back in the same microtask batch).
	private messageProcessingQueues: Map<string, Promise<void>> = new Map();
	/** Recent compact milestones, partitioned by agent session. */
	private operatorFeedbackBySession: Map<string, Map<string, number>> =
		new Map();
	private lastOperatorProgressAt: Map<string, number> = new Map();
	private operatorHeartbeatTimers: Map<string, ReturnType<typeof setInterval>> =
		new Map();
	/** The OpenCode turn currently attached to a Linear session. */
	private openCodeTurnBySession: Map<string, number> = new Map();
	/**
	 * Lifecycle notices already posted for a turn. These are intentionally
	 * bounded: a noisy runner must not turn Linear into a log stream.
	 */
	private openCodeLifecycleNotices: Set<string> = new Set();
	/** Per-session projection state; never shared between concurrent sessions. */
	private observabilityBySession: Map<string, SessionObservabilityProjection> =
		new Map();
	private getParentSessionId?: (childSessionId: string) => string | undefined;
	private resumeParentSession?: (
		parentSessionId: string,
		prompt: string,
		childSessionId: string,
	) => Promise<void>;

	constructor(
		getParentSessionId?: (childSessionId: string) => string | undefined,
		resumeParentSession?: (
			parentSessionId: string,
			prompt: string,
			childSessionId: string,
		) => Promise<void>,
		logger?: ILogger,
		persistState?: () => Promise<void>,
	) {
		super();
		this.logger = logger ?? createLogger({ component: "AgentSessionManager" });
		this.getParentSessionId = getParentSessionId;
		this.resumeParentSession = resumeParentSession;
		this.persistState = persistState;
	}

	/**
	 * Replaces the durable-state flush awaited by {@link emitTerminalOnce}.
	 * Normally supplied via the constructor; exposed for tests that need to
	 * observe or fail the flush.
	 */
	setPersistStateHook(persistState: () => Promise<void>): void {
		this.persistState = persistState;
	}

	/**
	 * Register an activity sink for a specific session.
	 * This associates the session with the correct issue tracker for activity posting.
	 */
	setActivitySink(sessionId: string, sink: IActivitySink): void {
		this.activitySinks.set(sessionId, sink);
	}

	/**
	 * Get the activity sink for a session.
	 */
	private getActivitySink(sessionId: string): IActivitySink | undefined {
		return this.activitySinks.get(sessionId);
	}

	/**
	 * Get a session-scoped logger with context (sessionId, platform, issueIdentifier).
	 */
	private sessionLog(sessionId: string): ILogger {
		const session = this.sessions.get(sessionId);
		return this.logger.withContext({
			sessionId,
			platform: session?.issueContext?.trackerId,
			issueIdentifier: session?.issueContext?.issueIdentifier,
		});
	}

	/**
	 * Initialize an agent session from webhook
	 * The session is already created by the platform, we just need to track it
	 *
	 * @param sessionId - Internal session ID
	 * @param issueId - Issue/PR identifier
	 * @param issueMinimal - Minimal issue data
	 * @param workspace - Workspace configuration
	 * @param platform - Source platform ("linear", "github", "gitlab", "slack"). Defaults to "linear".
	 *                   Only "linear" sessions will have activities streamed to Linear.
	 * @param repositories - Repository contexts for the session (defaults to empty array)
	 */
	createCyrusAgentSession(
		sessionId: string,
		issueId: string,
		issueMinimal: IssueMinimal,
		workspace: Workspace,
		platform: "linear" | "github" | "gitlab" | "slack" = "linear",
		repositories: RepositoryContext[] = [],
		creator?: SessionCreator,
	): CyrusAgentSession {
		const log = this.logger.withContext({
			sessionId,
			platform,
			issueIdentifier: issueMinimal.identifier,
		});
		log.info(`Tracking session for issue ${issueId}`);

		const agentSession: CyrusAgentSession = {
			id: sessionId,
			// Only Linear sessions have a valid external session ID for posting activities
			externalSessionId: platform === "linear" ? sessionId : undefined,
			type: AgentSessionType.CommentThread,
			status: AgentSessionStatus.Active,
			context: AgentSessionType.CommentThread,
			createdAt: Date.now(),
			updatedAt: Date.now(),
			issueContext: {
				trackerId: platform,
				issueId: issueId,
				issueIdentifier: issueMinimal.identifier,
			},
			issueId, // Kept for backwards compatibility
			issue: issueMinimal,
			repositories,
			workspace: workspace,
			creator,
		};

		// Store locally
		this.sessions.set(sessionId, agentSession);
		this.entries.set(sessionId, []);

		return agentSession;
	}

	/**
	 * Create an agent session for chat-style platforms (Slack, etc.) that are
	 * not tied to a specific issue or repository.
	 *
	 * Unlike {@link createCyrusAgentSession}, this does NOT require issue
	 * context — the session lives in a standalone workspace with no issue
	 * tracker linkage.
	 *
	 * @param repositories - Repository contexts for the session (defaults to empty array for chatbot sessions)
	 */
	createChatSession(
		sessionId: string,
		workspace: Workspace,
		platform: string,
		repositories: RepositoryContext[] = [],
	): CyrusAgentSession {
		const log = this.logger.withContext({ sessionId, platform });
		log.info("Creating chat session");

		const agentSession: CyrusAgentSession = {
			id: sessionId,
			type: AgentSessionType.CommentThread,
			status: AgentSessionStatus.Active,
			context: AgentSessionType.CommentThread,
			createdAt: Date.now(),
			updatedAt: Date.now(),
			repositories,
			workspace,
		};

		this.sessions.set(sessionId, agentSession);
		this.entries.set(sessionId, []);

		return agentSession;
	}

	/**
	 * Update Agent Session with session ID from system initialization
	 * Automatically detects whether it's Claude or Gemini based on the runner
	 */
	updateAgentSessionWithRunnerSessionId(
		sessionId: string,
		claudeSystemMessage: SDKSystemMessage,
	): void {
		const linearSession = this.sessions.get(sessionId);
		if (!linearSession) {
			const log = this.sessionLog(sessionId);
			log.warn(`No session found`);
			return;
		}

		// Determine which runner is being used
		const runner = linearSession.agentRunner;
		const runnerType =
			runner?.constructor.name === "GeminiRunner"
				? "gemini"
				: runner?.constructor.name === "CodexRunner"
					? "codex"
					: runner?.constructor.name === "CursorRunner"
						? "cursor"
						: runner?.constructor.name === "OpenCodeRunner"
							? "opencode"
							: "claude";

		// Update the appropriate session ID based on runner type — but only when
		// the runner says the id is one a later turn can actually resume.
		if (!isResumableRunnerSessionId(runner)) {
			this.sessionLog(sessionId).warn(
				`Not storing a ${runnerType} session id from a session that failed before the agent started; the next turn will start a fresh one`,
			);
		} else if (runnerType === "gemini") {
			linearSession.geminiSessionId = claudeSystemMessage.session_id;
		} else if (runnerType === "codex") {
			linearSession.codexSessionId = claudeSystemMessage.session_id;
		} else if (runnerType === "cursor") {
			linearSession.cursorSessionId = claudeSystemMessage.session_id;
		} else if (runnerType === "opencode") {
			linearSession.opencodeSessionId = claudeSystemMessage.session_id;
		} else {
			linearSession.claudeSessionId = claudeSystemMessage.session_id;
		}

		linearSession.updatedAt = Date.now();
		linearSession.metadata = {
			...linearSession.metadata, // Preserve existing metadata
			model: claudeSystemMessage.model,
			tools: claudeSystemMessage.tools,
			permissionMode: claudeSystemMessage.permissionMode,
			apiKeySource: claudeSystemMessage.apiKeySource,
		};
	}

	/**
	 * Create a session entry from user/assistant message (without syncing to Linear)
	 */
	private async createSessionEntry(
		sessionId: string,
		sdkMessage: SDKUserMessage | SDKAssistantMessage,
	): Promise<CyrusAgentSessionEntry> {
		// Extract tool info if this is an assistant message
		const toolInfo =
			sdkMessage.type === "assistant" ? this.extractToolInfo(sdkMessage) : null;
		// Extract tool_use_id and error status if this is a user message with tool_result
		const toolResultInfo =
			sdkMessage.type === "user"
				? this.extractToolResultInfo(sdkMessage)
				: null;
		// Extract SDK error from assistant messages (e.g., rate_limit, billing_error)
		// SDKAssistantMessage has optional `error?: SDKAssistantMessageError` field
		// See: @anthropic-ai/claude-agent-sdk sdk.d.ts lines 1013-1022
		// Evidence from ~/.cyrus/logs/CYGROW-348 session jsonl shows assistant messages with
		// "error":"rate_limit" field when usage limits are hit
		const sdkError =
			sdkMessage.type === "assistant" ? sdkMessage.error : undefined;

		// Determine which runner is being used
		const session = this.sessions.get(sessionId);
		const runner = session?.agentRunner;
		const runnerType =
			runner?.constructor.name === "GeminiRunner"
				? "gemini"
				: runner?.constructor.name === "CodexRunner"
					? "codex"
					: runner?.constructor.name === "CursorRunner"
						? "cursor"
						: runner?.constructor.name === "OpenCodeRunner"
							? "opencode"
							: "claude";

		const sessionEntry: CyrusAgentSessionEntry = {
			// Set the appropriate session ID based on runner type
			...(runnerType === "gemini"
				? { geminiSessionId: sdkMessage.session_id }
				: runnerType === "codex"
					? { codexSessionId: sdkMessage.session_id }
					: runnerType === "cursor"
						? { cursorSessionId: sdkMessage.session_id }
						: runnerType === "opencode"
							? { opencodeSessionId: sdkMessage.session_id }
							: { claudeSessionId: sdkMessage.session_id }),
			type: sdkMessage.type,
			content: this.extractContent(sdkMessage),
			metadata: {
				timestamp: Date.now(),
				parentToolUseId: sdkMessage.parent_tool_use_id || undefined,
				...(toolInfo && {
					toolUseId: toolInfo.id,
					toolName: toolInfo.name,
					toolInput: toolInfo.input,
				}),
				...(toolResultInfo && {
					toolUseId: toolResultInfo.toolUseId,
					toolResultError: toolResultInfo.isError,
				}),
				...(sdkError && { sdkError }),
			},
		};

		// DON'T store locally yet - wait until we actually post to Linear
		return sessionEntry;
	}

	/**
	 * Complete a session from Claude result message.
	 * Posts the final result to the issue tracker and handles child session completion.
	 */
	async completeSession(
		sessionId: string,
		resultMessage: SDKResultMessage,
	): Promise<void> {
		const session = this.sessions.get(sessionId);
		if (!session) {
			const log = this.sessionLog(sessionId);
			log.error(`No session found`);
			return;
		}

		const log = this.sessionLog(sessionId);
		const isOpenCode = this.isOpenCodeSession(sessionId);

		// Clear any active Task when session completes
		this.activeTasksBySession.delete(sessionId);
		this.stopOperatorHeartbeat(sessionId);

		const wasStopRequested = this.consumeStopRequest(sessionId);
		const status = wasStopRequested
			? AgentSessionStatus.Error
			: resultMessage.subtype === "success"
				? AgentSessionStatus.Complete
				: AgentSessionStatus.Error;

		// Update session status and metadata
		await this.updateSessionStatus(sessionId, status, {
			totalCostUsd: resultMessage.total_cost_usd,
			usage: resultMessage.usage,
		});

		const terminalState: "complete" | "error" | "stopped" = wasStopRequested
			? "stopped"
			: status === AgentSessionStatus.Complete
				? "complete"
				: "error";

		if (wasStopRequested) {
			// Post before going terminal. Linear derives session state from the last
			// emitted activity, so returning without one leaves the session at
			// `active` ("Working...") even though the runner is gone. Best-effort:
			// a post failure must not strand the issue lock, which only the
			// terminal signal releases.
			try {
				if (isOpenCode) {
					await this.postOpenCodeLifecycleNotice(
						sessionId,
						"operator-stop",
						"OpenCode was stopped by an operator.",
					);
				}
				await this.createErrorActivity(sessionId, "Session stopped by user.");
			} catch (err) {
				log.error("Failed to post stop activity:", err);
			}
			await this.emitTerminalOnce(sessionId, terminalState);
			log.info(`Session was stopped by user`);
			return;
		}

		// Assigned inside the `try` below, and read from the `finally`. A throw
		// before it is set therefore leaves it null and the terminal state still
		// fires — a lost result entry is recoverable, an issue locked until an
		// admin runs `cyrus router unlock` is not.
		let pendingWork: AgentPendingWork | null = null;

		try {
			if (isOpenCode) {
				await this.postOpenCodeLifecycleNotice(
					sessionId,
					resultMessage.subtype === "success" ? "completed" : "failed",
					resultMessage.subtype === "success"
						? "OpenCode completed this turn."
						: "OpenCode ended unexpectedly.",
				);
			}
			// Post final result to issue tracker.
			await this.addResultEntry(sessionId, resultMessage);

			// When the turn ended with work still scheduled or in flight
			// (ScheduleWakeup/cron timers, backgrounded tasks), the runner holds
			// its session open and the wakeup will stream new messages in later.
			// Post a thought AFTER the response so Linear's agent panel returns
			// to its working state and the user can see what the session is
			// waiting on.
			//
			// Sampled here rather than before the posts: a background task that
			// finished while we were writing to Linear should let the session go
			// terminal now, not defer it to a wakeup that may never arrive. Only
			// on `success` — an errored result ends the session regardless, and
			// deferring on it would strand the lock.
			if (resultMessage.subtype === "success") {
				pendingWork = this.getRunnerPendingWork(sessionId);
				if (pendingWork) {
					const thoughtBody = formatPendingWorkThought(pendingWork);
					if (thoughtBody) {
						await this.createThoughtActivity(sessionId, thoughtBody);
						log.info(
							`Posted pending-work thought (${pendingWork.sessionCrons.length} crons, ${pendingWork.backgroundTasks.length} background tasks)`,
						);
					}
				}
			}

			// Handle child session completion. It posts to the *parent* session,
			// never to this one, so it does not strictly need to precede the
			// terminal signal — but keeping it inside the `try` means a throw here
			// still releases this session's lock.
			//
			// A session held open for pending work is not done yet: the wakeup or
			// background task will stream more messages in, ending in another
			// result. Resuming the parent now would hand it a non-final result and
			// resume it again later, so defer to the result that actually ends the
			// session — the same condition that defers the terminal signal below.
			const parentSessionId = this.getParentSessionId?.(sessionId);
			if (parentSessionId && this.resumeParentSession) {
				if (pendingWork) {
					log.info(
						`Child session has pending work; deferring parent ${parentSessionId} resume until the session finishes`,
					);
				} else {
					await this.handleChildSessionCompletion(sessionId, resultMessage);
				}
			}
		} finally {
			// Notify terminal-state observers (router mode releases the issue lock +
			// session affinity via RouterConnection.sendSessionState) — LAST, and only
			// once the session has genuinely finished.
			//
			// Two ordering rules, both learned the hard way:
			//
			// 1. Emit *after* every write above. The router drops this device's
			//    ownership of the session the instant it sees the terminal state, so
			//    anything posted afterwards is rejected with "session not owned by this
			//    device". Emitting first cost every completing session its final result.
			//
			// 2. An `SDKResultMessage` is only *turn*-terminal. With pending work the
			//    runner keeps its session open and streams more messages in on the
			//    wakeup, so going terminal here would strand the rest of the run
			//    unowned. Stay non-terminal; the wakeup's own result lands back here
			//    with no pending work and emits then. A session killed before that
			//    reaches abortSession(), which emits instead — so the lock cannot leak.
			//
			// This covers only terminations where the SDK yielded a result — i.e. a
			// normal finish, an error, or an *interrupt* (which lets the query return).
			// A stop that kills the query outright never reaches here; EdgeWorker calls
			// abortSession() on that path instead. See the note on abortSession().
			if (!pendingWork) {
				await this.emitTerminalOnce(sessionId, terminalState);
			} else {
				// An EVENT, not the `info` it was. This branch withholds the only
				// thing that releases the router's issue lock, and it does so with no
				// bound: the signal is retried only if a wakeup or a background task
				// yields another result, so a background task that never exits holds
				// the issue forever. That made it the prime suspect for CAN-133 —
				// and it was unfalsifiable, because a sandbox worker forwards WARN+
				// by default, so the `info` never left the container while `event()`
				// bypasses that threshold entirely (NOR-402).
				log.event(
					CYRUS_EVENTS.sessionTerminalDeferred,
					cyrusAttributes({
						...this.runFactAttributes(sessionId),
						agent_session_id: sessionId,
						terminal_state: terminalState,
						session_cron_count: pendingWork.sessionCrons.length,
						background_task_count: pendingWork.backgroundTasks.length,
						live_background_task_count:
							pendingWork.liveBackgroundTasks?.length ?? 0,
						// The identity of what is holding the session open. Without it
						// the event says a session is deferred and gives an operator
						// nothing to act on.
						//
						// NOT `formatPendingWorkThought`: that renders the user-facing
						// "standing by" message, which lists only scheduled wakeups and
						// returns null for a session held open solely by a LIVE
						// background task — i.e. it is null for precisely the case that
						// is the leading suspect for a session that never terminates.
						pending_work: formatPendingWorkSummary(pendingWork),
					}),
				);
				log.info(
					`Deferring terminal signal: runner has pending work (${pendingWork.sessionCrons.length} crons, ${pendingWork.backgroundTasks.length} background tasks)`,
				);
				// The run is still ACTIVE and this says so explicitly. Without it the
				// router's last word on a session held open for hours by a cron is
				// whatever it happened to be doing before the turn ended — which is
				// indistinguishable from a worker that stopped reporting, and is how
				// a healthy long-running session comes to look stranded.
				this.emit(
					"sessionPendingWork",
					sessionId,
					pendingWork.sessionCrons.length +
						pendingWork.backgroundTasks.length +
						(pendingWork.liveBackgroundTasks?.length ?? 0),
				);
			}
		}

		log.info(`Session completed (subtype: ${resultMessage.subtype})`);
	}

	/**
	 * Pending work (scheduled wakeups/crons, in-flight background tasks) for
	 * the session's runner, or null when the runner doesn't support pending
	 * work reporting or nothing is pending.
	 */
	private getRunnerPendingWork(sessionId: string): AgentPendingWork | null {
		const runner = this.sessions.get(sessionId)?.agentRunner;
		if (!runner?.getPendingWork) return null;
		const pendingWork = runner.getPendingWork();
		return pendingWork.sessionCrons.length > 0 ||
			pendingWork.backgroundTasks.length > 0 ||
			(pendingWork.liveBackgroundTasks?.length ?? 0) > 0
			? pendingWork
			: null;
	}

	/**
	 * Whether this session has any work that will wake it later.
	 *
	 * Used as the "safe to park?" gate: a session blocked on a user answer with
	 * a background build still running must NOT be parked, because suspending
	 * the container freezes that build — and its completion, which would
	 * normally wake the session, can then never arrive.
	 *
	 * A runner that reports nothing (an older runner, or one without
	 * `getPendingWork`) reads as "no pending work". That is deliberate: the
	 * conservative alternative would block parking forever for those runners.
	 */
	hasPendingWork(sessionId: string): boolean {
		return this.getRunnerPendingWork(sessionId) !== null;
	}

	/**
	 * How many things will wake this session later — scheduled wakeups and
	 * crons, backgrounded tasks, and tasks still live.
	 *
	 * Reported to the router as an ACTIVE-run fact, never as a wait reason: a
	 * run carrying pending work is working, and a seven-hour cron run must stay
	 * active and observable rather than being labelled waiting or failed.
	 *
	 * A runner without `getPendingWork` reads as 0, the same way
	 * {@link hasPendingWork} reads it as "nothing pending" — see there for why
	 * the conservative alternative is worse.
	 */
	pendingWorkCount(sessionId: string): number {
		const pending = this.getRunnerPendingWork(sessionId);
		if (!pending) return 0;
		return (
			pending.sessionCrons.length +
			pending.backgroundTasks.length +
			(pending.liveBackgroundTasks?.length ?? 0)
		);
	}

	/**
	 * The execution identity of a session's current turn, as far as it is known.
	 *
	 * Both fields are optional and neither is guessed. The model is only known
	 * once the runner's init message has arrived, and a session parked before
	 * that reports no model rather than a placeholder — an invented value in a
	 * column an operator filters on is worse than an absent one.
	 */
	getRunFacts(sessionId: string): { runner?: string; model?: string } {
		const session = this.sessions.get(sessionId);
		if (!session) return {};
		const runner = session.agentRunner
			? runnerTypeOf(session.agentRunner)
			: undefined;
		return {
			...(runner ? { runner } : {}),
			...(session.metadata?.model ? { model: session.metadata.model } : {}),
		};
	}

	/**
	 * The canonical attributes for a worker-emitted lifecycle event (CYR-72).
	 *
	 * Deliberately only the three facts the WORKER is the authority on:
	 *
	 *  - `session_id` — the Linear agent session id, which is the join key every
	 *    other canonical column hangs off. Note this is NOT the same id as
	 *    `agent_session_id` on the runner's own `session.*` events: that one is
	 *    the agent SDK's session id, the two families do not join, and a KQL
	 *    query that mixes them returns nothing rather than erroring. Both are
	 *    emitted so either question can be asked.
	 *  - `runner` / `model` — execution identity the worker is the ORIGINAL
	 *    authority on. The router's copy is only a cache of what a worker
	 *    previously reported on a frame, so `SandboxLogRelay` reads these two off
	 *    the line when its own copy is null rather than overwriting them — which
	 *    is what makes them useful in the window before the first frame carrying
	 *    them lands. Every other canonical key the router claims outright, so a
	 *    worker copy of it would be discarded; these two are the deliberate
	 *    exception and the relay documents why.
	 *
	 * The workspace, owner, team, project, run id and device id are deliberately
	 * ABSENT rather than guessed. The worker has no trustworthy view of them, and
	 * the relay stamps them from the authenticated device and run rows — a
	 * worker-supplied copy would be discarded there and would only be believed on
	 * the one deployment where nothing checks it.
	 *
	 * The two run facts are omitted when unknown rather than sent as null: the
	 * relay's null-vs-absent distinction is what lets it tell "the worker did not
	 * report one" from "the worker reported nothing at all", and a relayed line
	 * gets the full null-filled canonical set from the router regardless.
	 */
	private runFactAttributes(sessionId: string): LogEventAttributes {
		const facts = this.getRunFacts(sessionId);
		return {
			session_id: sessionId,
			...(facts.runner ? { runner: facts.runner } : {}),
			...(facts.model ? { model: facts.model } : {}),
		};
	}

	private consumeStopRequest(linearAgentActivitySessionId: string): boolean {
		if (!this.stopRequestedSessions.has(linearAgentActivitySessionId)) {
			return false;
		}

		this.stopRequestedSessions.delete(linearAgentActivitySessionId);
		return true;
	}

	requestSessionStop(linearAgentActivitySessionId: string): void {
		this.stopRequestedSessions.add(linearAgentActivitySessionId);
	}

	/**
	 * Drops a pending stop request without acting on it.
	 *
	 * {@link consumeStopRequest} only fires from {@link completeSession}, which is
	 * reached exclusively when a *live* runner yields a result. A stop delivered
	 * to a session whose runner already died (OOM kill, crash) therefore latches
	 * forever, and the next resumed turn consumes it instead — silently
	 * swallowing the user's prompt and reporting the session as stopped.
	 *
	 * A prompt is an explicit instruction to continue, so any stop request that
	 * predates it is stale. Called from `EdgeWorker.resumeAgentSession`.
	 */
	clearStopRequest(linearAgentActivitySessionId: string): void {
		this.stopRequestedSessions.delete(linearAgentActivitySessionId);
	}

	/**
	 * Emits "sessionTerminal" at most once per session.
	 *
	 * {@link completeSession} and {@link abortSession} are independent entry
	 * points into the terminal path, and a killed query could still — in
	 * principle — surface a late result. Router mode's lock release is
	 * idempotent, so a duplicate would be harmless there, but other observers
	 * should not have to assume that.
	 *
	 * The state flush that precedes the emit is load-bearing, and is the third
	 * ordering rule of the terminal path (see {@link completeSession} for the
	 * other two). Both observers consume durable state, not in-memory state:
	 * the router's `session_state` frame releases the issue lock and session
	 * affinity — after which this device may be idle-stopped or destroyed at any
	 * time — and the persistence floor builds its bundle by re-reading
	 * `edge-worker-state.json` from disk. Emitting before the flush therefore
	 * publishes a snapshot in which this session still reads `active` with no
	 * terminal state and without its final response entry. A cold restore from
	 * that snapshot cannot tell the session apart from one whose host died
	 * mid-run, so {@link reconcileInterruptedSessions} posts a spurious `error`
	 * activity and emits a stale terminal frame over a session that had in fact
	 * finished cleanly.
	 */
	private async emitTerminalOnce(
		sessionId: string,
		state: "complete" | "error" | "stopped",
		opts?: { force?: boolean },
	): Promise<void> {
		// `force` is for an explicit, user-driven stop that arrives after the
		// session already went terminal. The guard below exists to swallow a late
		// *duplicate result* for work that has already been reported; a fresh stop
		// signal is a different event. The router re-establishes this device's
		// session affinity when it routes the stop's `prompted` webhook, so
		// staying silent leaves an affinity row that nothing will ever clear —
		// and ContainerLifecycle never reclaims a device with affinity > 0
		// (PAR-146). The flag is deliberately NOT a reset: the session stays in
		// `terminalEmittedSessions`, so a later duplicate result is still
		// swallowed exactly as before.
		if (this.terminalEmittedSessions.has(sessionId) && !opts?.force) return;
		this.terminalEmittedSessions.add(sessionId);

		const session = this.sessions.get(sessionId);
		if (session) {
			session.terminalState = state;
			session.updatedAt = Date.now();
		}

		// Best-effort: a failed flush must not stop the terminal signal, which is
		// the only thing that releases the router's issue lock and affinity.
		if (this.persistState) {
			try {
				await this.persistState();
			} catch (err) {
				this.sessionLog(sessionId).error(
					"Failed to flush state before signalling terminal:",
					err,
				);
			}
		}

		// The counterpart to `session.terminal_deferred`. Between them an operator
		// can answer "did this session ever finish?" from the log stream alone —
		// which for CAN-133 took reading five hours of gauge samples and inferring
		// it from an affinity count that never dropped.
		this.sessionLog(sessionId).event(
			CYRUS_EVENTS.sessionTerminalSignalled,
			cyrusAttributes({
				// Captured before the session row is torn down, so the runner and
				// model on the closing record describe the turn that just ended
				// rather than reading as absent.
				...this.runFactAttributes(sessionId),
				agent_session_id: sessionId,
				terminal_state: state,
				forced: opts?.force ?? false,
			}),
		);
		this.emit("sessionTerminal", sessionId, state);
	}

	/**
	 * Marks a session terminal when it was killed without the SDK ever yielding
	 * an `SDKResultMessage`.
	 *
	 * `completeSession` — the only other place "sessionTerminal" is emitted — is
	 * reached exclusively from the `case "result"` branch of the message loop. So
	 * a stop that calls `AgentRunner.stop()` (a non-warm runner, or the
	 * double-stop full abort) tears the query down before any result arrives, and
	 * the terminal signal is never sent. In router mode that stranded the issue
	 * lock and session affinity on the router **permanently**: the router's sweep
	 * only reclaims locks from devices that go offline past the event TTL, so a
	 * device that stayed connected held the issue until an admin ran
	 * `cyrus router unlock`. `requestSessionStop()` alone does not help — it only
	 * sets a flag that `completeSession` consumes, and `completeSession` never
	 * runs on this path.
	 */
	async abortSession(
		linearAgentActivitySessionId: string,
		opts?: { force?: boolean },
	): Promise<void> {
		this.activeTasksBySession.delete(linearAgentActivitySessionId);
		this.stopOperatorHeartbeat(linearAgentActivitySessionId);
		if (this.isOpenCodeSession(linearAgentActivitySessionId)) {
			await this.postOpenCodeLifecycleNotice(
				linearAgentActivitySessionId,
				"operator-stop",
				"OpenCode was stopped by an operator.",
				"error",
			);
		}
		await this.emitTerminalOnce(linearAgentActivitySessionId, "stopped", opts);
	}

	/**
	 * Marks a session terminal because it failed before the agent ever ran.
	 *
	 * A session that throws on the way up never reaches the message loop, so
	 * neither `completeSession` (result branch only) nor `abortSession` (an
	 * explicit stop) fires. The rejection propagates as far as
	 * `EdgeWorker.handleWebhook`, whose catch deliberately does not rethrow so a
	 * bad webhook cannot take the process down — and there it stops, as one log
	 * line. Meanwhile `postInstantAcknowledgment` has already posted a thought,
	 * so Linear renders the session as working, forever: no activity, no terminal
	 * state, and the router's issue lock and session affinity still pinned
	 * because only the terminal signal releases them. That is the NOR-402 shape.
	 *
	 * The error activity is best-effort for the same reason it is in
	 * `completeSession`: a failed post must not cost us the terminal signal.
	 * `emitTerminalOnce` is idempotent, so calling this on a session that somehow
	 * did reach a terminal state is a no-op rather than a duplicate frame.
	 */
	async failSession(sessionId: string, body: string): Promise<void> {
		const log = this.sessionLog(sessionId);
		this.activeTasksBySession.delete(sessionId);
		this.stopOperatorHeartbeat(sessionId);
		await this.updateSessionStatus(sessionId, AgentSessionStatus.Error);

		try {
			if (this.isOpenCodeSession(sessionId)) {
				await this.postOpenCodeLifecycleNotice(
					sessionId,
					"failed",
					"OpenCode ended unexpectedly.",
				);
			}
			await this.createErrorActivity(sessionId, body);
		} catch (err) {
			log.error("Failed to post session-failure activity:", err);
		}

		await this.emitTerminalOnce(sessionId, "error");
	}

	/**
	 * Handle child session completion and resume parent
	 */
	private async handleChildSessionCompletion(
		sessionId: string,
		resultMessage: SDKResultMessage,
	): Promise<void> {
		const log = this.sessionLog(sessionId);
		if (!this.getParentSessionId || !this.resumeParentSession) {
			return;
		}

		const parentAgentSessionId = this.getParentSessionId(sessionId);

		if (!parentAgentSessionId) {
			log.error(`No parent session ID found for child session`);
			return;
		}

		log.info(
			`Child session completed, resuming parent ${parentAgentSessionId}`,
		);

		try {
			const childResult =
				"result" in resultMessage
					? resultMessage.result
					: "No result available";
			const promptToParent = `Child agent session ${sessionId} completed with result:\n\n${childResult}`;

			await this.resumeParentSession(
				parentAgentSessionId,
				promptToParent,
				sessionId,
			);

			log.info(`Successfully resumed parent session ${parentAgentSessionId}`);
		} catch (error) {
			log.error(`Failed to resume parent session:`, error);
		}
	}

	/**
	 * Handle streaming Claude messages and route to appropriate methods.
	 *
	 * Serializes processing per session so concurrent onMessage callbacks from
	 * the runner (which is fire-and-forget) do not interleave their async work.
	 * Without this serialization, a tool_result message could run its handler
	 * ahead of the matching tool_use registration in toolCallsByToolUseId,
	 * producing a fallback action="Tool" activity in Linear (seen with parallel
	 * deferred tools like ToolSearch).
	 */
	async handleClaudeMessage(
		sessionId: string,
		message: SDKMessage,
	): Promise<void> {
		const prev =
			this.messageProcessingQueues.get(sessionId) ?? Promise.resolve();
		const next = prev.then(() => this.processClaudeMessage(sessionId, message));
		// Swallow errors in the chained promise so one failure does not block
		// future messages for this session. The concrete handler already logs
		// errors internally.
		this.messageProcessingQueues.set(
			sessionId,
			next.catch(() => undefined),
		);
		return next;
	}

	/**
	 * Actual message dispatch. Invoked only via the per-session queue in
	 * handleClaudeMessage so at most one instance runs for a given session.
	 */
	private async processClaudeMessage(
		sessionId: string,
		message: SDKMessage,
	): Promise<void> {
		const log = this.sessionLog(sessionId);
		const session = this.sessions.get(sessionId);
		try {
			// OpenCode's existing heartbeat needs a current output timestamp to
			// distinguish a quiet-but-alive process from a stale stream. Recording
			// this fact is silent; the heartbeat or an explicit health signal owns
			// Linear-facing status activity so established timelines stay unchanged.
			if (this.isOpenCodeSession(sessionId)) {
				this.getObservabilityProjection(sessionId).recordRunner({
					type: "output",
				});
			}
			switch (message.type) {
				case "system":
					if (message.subtype === "init") {
						const wasOpenCodeResume = Boolean(session?.opencodeSessionId);
						this.updateAgentSessionWithRunnerSessionId(sessionId, message);
						if (
							session?.agentRunner &&
							runnerTypeOf(session.agentRunner) === "opencode"
						) {
							await this.postOpenCodeLifecycleNotice(
								sessionId,
								wasOpenCodeResume ? "resumed" : "started",
								wasOpenCodeResume
									? "OpenCode resumed work on this request."
									: "OpenCode started work on this request.",
							);
						}

						// Post model notification
						const systemMessage = message as SDKSystemMessage;
						if (systemMessage.model) {
							await this.postModelNotificationThought(
								sessionId,
								systemMessage.model,
							);
						}
					} else if (message.subtype === "status") {
						// Handle status updates (compacting, etc.)
						await this.handleStatusMessage(
							sessionId,
							message as SDKStatusMessage,
						);
					}
					break;

				case "user": {
					const userEntry = await this.createSessionEntry(
						sessionId,
						message as SDKUserMessage,
					);
					await this.syncEntryToActivitySink(userEntry, sessionId);
					if (
						session?.agentRunner &&
						runnerTypeOf(session.agentRunner) === "opencode" &&
						userEntry.metadata?.toolUseId
					) {
						await this.postOpenCodeCheckpoint(
							sessionId,
							`tool:${userEntry.metadata.toolUseId}:result`,
							"OpenCode completed a tool step.",
						);
					}
					break;
				}

				case "assistant": {
					const assistantEntry = await this.createSessionEntry(
						sessionId,
						message as SDKAssistantMessage,
					);
					// Buffer the text content so addResultEntry can post it as the response.
					// Track whether this body is a tool_use input (JSON) rather than real
					// assistant prose, so addResultEntry never posts raw tool JSON as the
					// final "response" when a turn ends on a tool call (CYPACK-1177).
					if (assistantEntry.content) {
						this.lastAssistantBodyBySession.set(
							sessionId,
							assistantEntry.content,
						);
						this.lastAssistantBodyIsToolInputBySession.set(
							sessionId,
							!!assistantEntry.metadata?.toolUseId,
						);
					}
					if (assistantEntry.metadata?.toolUseId) {
						// Tool-use message: flush any buffered text first (preserves ordering),
						// then post immediately for real-time "in progress" display
						await this.flushBufferedAssistant(sessionId);
						await this.syncEntryToActivitySink(assistantEntry, sessionId);
						if (
							session?.agentRunner &&
							runnerTypeOf(session.agentRunner) === "opencode"
						) {
							await this.postOpenCodeCheckpoint(
								sessionId,
								`tool:${assistantEntry.metadata.toolUseId}:start`,
								"OpenCode started a tool step.",
							);
						}
					} else {
						// Text-only message: buffer it so the LAST one can be posted as "response"
						// Flush any previous buffered text first (posts as thought)
						await this.flushBufferedAssistant(sessionId);
						// Skip empty/whitespace-only text turns — otherwise they post as
						// blank thoughts in Linear, showing up as an extra blank line
						// between activities (e.g. between "Using model: ..." and the
						// first real assistant turn).
						if (assistantEntry.content?.trim()) {
							this.bufferedAssistantEntryBySession.set(
								sessionId,
								assistantEntry,
							);
						}
					}
					break;
				}

				case "result":
					// Result arrived: discard buffered entry (addResultEntry uses lastAssistantBodyBySession
					// to post the content as a response activity)
					this.bufferedAssistantEntryBySession.delete(sessionId);
					await this.completeSession(sessionId, message as SDKResultMessage);
					break;

				case "rate_limit_event":
					this.handleRateLimitEvent(sessionId, message as SDKRateLimitEvent);
					break;

				default:
					log.warn(`Unknown message type: ${(message as any).type}`);
			}
		} catch (error) {
			log.error(`Error handling message:`, error);
			// Mark session as error state
			await this.updateSessionStatus(sessionId, AgentSessionStatus.Error);
		}
	}

	/**
	 * Flush the buffered assistant entry as thought/action (non-result flush).
	 * Called when a new message arrives before result, to post the previous
	 * assistant message as a thought/action activity.
	 */
	private async flushBufferedAssistant(sessionId: string): Promise<void> {
		const buffered = this.bufferedAssistantEntryBySession.get(sessionId);
		if (!buffered) return;
		this.bufferedAssistantEntryBySession.delete(sessionId);
		// Defensive guard: never post a blank thought — it would appear as an
		// empty line between real activities in Linear.
		if (!buffered.content?.trim()) return;
		await this.syncEntryToActivitySink(buffered, sessionId);
	}

	/**
	 * Handle rate limit events from Claude runners
	 */
	private handleRateLimitEvent(
		sessionId: string,
		message: SDKRateLimitEvent,
	): void {
		const log = this.sessionLog(sessionId);
		const info = message.rate_limit_info;

		if (info.status === "rejected") {
			const resetsAt = info.resetsAt
				? new Date(info.resetsAt * 1000).toISOString()
				: "unknown";
			log.warn(
				`Rate limited (${info.rateLimitType ?? "unknown"}), resets at ${resetsAt}`,
			);
		} else if (info.status === "allowed_warning") {
			log.info(
				`Rate limit warning: ${Math.round((info.utilization ?? 0) * 100)}% utilization (${info.rateLimitType ?? "unknown"})`,
			);
		}
		// "allowed" status is a no-op — fires frequently and provides no actionable information
	}

	/**
	 * Update session status and metadata
	 */
	private async updateSessionStatus(
		sessionId: string,
		status: AgentSessionStatus,
		additionalMetadata?: Partial<CyrusAgentSession["metadata"]>,
	): Promise<void> {
		const session = this.sessions.get(sessionId);
		if (!session) return;

		session.status = status;
		session.updatedAt = Date.now();

		if (additionalMetadata) {
			session.metadata = { ...session.metadata, ...additionalMetadata };
		}

		this.sessions.set(sessionId, session);
	}

	/**
	 * Add result entry from result message
	 */
	private async addResultEntry(
		sessionId: string,
		resultMessage: SDKResultMessage,
	): Promise<void> {
		// Determine which runner is being used
		const session = this.sessions.get(sessionId);
		const runner = session?.agentRunner;
		const runnerType =
			runner?.constructor.name === "GeminiRunner"
				? "gemini"
				: runner?.constructor.name === "CodexRunner"
					? "codex"
					: runner?.constructor.name === "CursorRunner"
						? "cursor"
						: runner?.constructor.name === "OpenCodeRunner"
							? "opencode"
							: "claude";

		// For error results, content may be in errors[] rather than result.
		const resultText =
			"result" in resultMessage && typeof resultMessage.result === "string"
				? resultMessage.result.trim()
				: "";

		// For success results, prefer the buffered last assistant message
		// (structured content) over result.result (a plain-text duplicate). But
		// when a turn ENDS on a tool call with no trailing assistant text, that
		// buffered body is the tool's raw input JSON — which must never be posted
		// as the Linear "response" (CYPACK-1177 / CYHOST-905: sessions showed a
		// "Finished" entry whose body was raw ScheduleWakeup / background-Bash
		// JSON).
		const bufferedAssistant = this.lastAssistantBodyBySession.get(sessionId);
		const bufferedIsToolInput =
			this.lastAssistantBodyIsToolInputBySession.get(sessionId) ?? false;
		this.lastAssistantBodyBySession.delete(sessionId);
		this.lastAssistantBodyIsToolInputBySession.delete(sessionId);

		let content: string;
		if (resultMessage.is_error) {
			content = (
				"errors" in resultMessage &&
				Array.isArray(resultMessage.errors) &&
				resultMessage.errors.length > 0
					? resultMessage.errors.join("\n")
					: resultText
			).trim();
			// A runner torn down mid-turn (OOM kill, SIGTERM) yields an error
			// result with neither `errors[]` nor `result` text. Falling through to
			// the empty-content guard below would post nothing at all, and Linear
			// derives session state from the last emitted activity — so the session
			// would sit at `active` ("Working...") forever, indistinguishable from
			// one still running. Synthesize a body so the `error` activity is
			// always posted.
			if (!content) {
				content = `Session ended unexpectedly (${resultMessage.subtype}).`;
			}
		} else if (bufferedIsToolInput) {
			// Turn ended on a tool call. Render a friendly response for a
			// ScheduleWakeup (gated on the runner actually reporting a pending
			// cron so a finished session is never rewritten); otherwise fall back
			// to the SDK's result text and, failing that, post nothing — the raw
			// tool JSON is never surfaced. Any pending work is declared by the
			// separate "Standing by" thought, so an empty response here is fine.
			const pendingWork = this.getRunnerPendingWork(sessionId);
			const wakeupInput =
				pendingWork && pendingWork.sessionCrons.length > 0
					? tryParseScheduleWakeupInput(bufferedAssistant ?? "")
					: null;
			content = wakeupInput
				? formatScheduleWakeupResponse(wakeupInput)
				: resultText;
		} else {
			content = (bufferedAssistant ?? resultText).trim();
		}

		// Never post an empty/blank "response" activity — that renders as a
		// bare "Finished" with no body. Skip it entirely (the timeline already
		// shows the trailing action, and pending work has its own thought).
		// An empty *error* never reaches here: it is given a synthesized body
		// above, because a silently-dropped error leaves the session "Working..."
		// in Linear rather than reporting the failure.
		if (!content.trim()) {
			return;
		}

		const resultEntry: CyrusAgentSessionEntry = {
			// Set the appropriate session ID based on runner type — omitted
			// entirely when the runner reports that it never established one (see
			// `isResumableRunnerSessionId`), so a failure that happened before the
			// agent started cannot leave a resumable-looking id behind.
			...(!isResumableRunnerSessionId(runner)
				? {}
				: runnerType === "gemini"
					? { geminiSessionId: resultMessage.session_id }
					: runnerType === "codex"
						? { codexSessionId: resultMessage.session_id }
						: runnerType === "cursor"
							? { cursorSessionId: resultMessage.session_id }
							: runnerType === "opencode"
								? { opencodeSessionId: resultMessage.session_id }
								: { claudeSessionId: resultMessage.session_id }),
			type: "result",
			content,
			metadata: {
				timestamp: Date.now(),
				durationMs: resultMessage.duration_ms,
				isError: resultMessage.is_error,
			},
		};

		// DON'T store locally - syncEntryToActivitySink will do it
		// Sync to Linear
		await this.syncEntryToActivitySink(resultEntry, sessionId);
	}

	/**
	 * Extract content from Claude message
	 */
	private extractContent(
		sdkMessage: SDKUserMessage | SDKAssistantMessage,
	): string {
		const message =
			sdkMessage.type === "user"
				? (sdkMessage.message as APIUserMessage)
				: (sdkMessage.message as APIAssistantMessage);

		if (typeof message.content === "string") {
			return message.content;
		}

		if (Array.isArray(message.content)) {
			return message.content
				.map((block) => {
					if (block.type === "text") {
						return block.text;
					} else if (block.type === "tool_use") {
						// For tool use blocks, return the input as JSON string
						return JSON.stringify(block.input, null, 2);
					} else if (block.type === "tool_result") {
						// For tool_result blocks, extract just the text content
						// Also store the error status in metadata if needed
						if ("is_error" in block && block.is_error) {
							// Mark this as an error result - we'll handle this elsewhere
						}
						if (typeof block.content === "string") {
							return block.content;
						}
						if (Array.isArray(block.content)) {
							return block.content
								.map((contentBlock: any) => {
									if (contentBlock.type === "text") {
										return contentBlock.text;
									}
									// ToolSearch emits tool_reference blocks; preserve the tool name
									// so the formatter can render "Loaded tools: `X`, `Y`".
									if (
										contentBlock.type === "tool_reference" &&
										contentBlock.tool_name
									) {
										return contentBlock.tool_name;
									}
									return "";
								})
								.filter(Boolean)
								.join("\n");
						}
						return "";
					}
					return "";
				})
				.filter(Boolean)
				.join("\n");
		}

		return "";
	}

	/**
	 * Extract tool information from Claude assistant message
	 */
	private extractToolInfo(
		sdkMessage: SDKAssistantMessage,
	): { id: string; name: string; input: any } | null {
		const message = sdkMessage.message as APIAssistantMessage;

		if (Array.isArray(message.content)) {
			const toolUse = message.content.find(
				(block) => block.type === "tool_use",
			);
			if (
				toolUse &&
				"id" in toolUse &&
				"name" in toolUse &&
				"input" in toolUse
			) {
				return {
					id: toolUse.id,
					name: toolUse.name,
					input: toolUse.input,
				};
			}
		}
		return null;
	}

	/**
	 * Extract tool_use_id and error status from Claude user message containing tool_result
	 */
	private extractToolResultInfo(
		sdkMessage: SDKUserMessage,
	): { toolUseId: string; isError: boolean } | null {
		const message = sdkMessage.message as APIUserMessage;

		if (Array.isArray(message.content)) {
			const toolResult = message.content.find(
				(block) => block.type === "tool_result",
			);
			if (toolResult && "tool_use_id" in toolResult) {
				return {
					toolUseId: toolResult.tool_use_id,
					isError: "is_error" in toolResult && toolResult.is_error === true,
				};
			}
		}
		return null;
	}

	/**
	 * Extract tool result content and error status from session entry
	 */
	private extractToolResult(
		entry: CyrusAgentSessionEntry,
	): { content: string; isError: boolean } | null {
		// Check if we have the error status in metadata
		const isError = entry.metadata?.toolResultError || false;

		return {
			content: entry.content,
			isError: isError,
		};
	}

	/**
	 * Sync session entry to external tracker (create AgentActivity)
	 */
	private async syncEntryToActivitySink(
		entry: CyrusAgentSessionEntry,
		sessionId: string,
	): Promise<void> {
		const log = this.sessionLog(sessionId);
		try {
			const session = this.sessions.get(sessionId);
			if (!session) {
				log.warn(`No session found`);
				return;
			}

			// Store entry locally first
			const entries = this.entries.get(sessionId) || [];
			entries.push(entry);
			this.entries.set(sessionId, entries);

			// Build activity content based on entry type
			let content: any;
			let ephemeral = false;
			switch (entry.type) {
				case "user": {
					const activeTaskId = this.activeTasksBySession.get(sessionId);
					if (activeTaskId && activeTaskId === entry.metadata?.toolUseId) {
						content = {
							type: "thought",
							body: `✅ Task Completed\n\n\n\n${entry.content}\n\n---\n\n`,
						};
						this.activeTasksBySession.delete(sessionId);
					} else if (entry.metadata?.toolUseId) {
						// This is a tool result - create an action activity with the result
						const toolResult = this.extractToolResult(entry);
						if (toolResult) {
							// Get the original tool information
							const originalTool = this.toolCallsByToolUseId.get(
								entry.metadata.toolUseId,
							);
							const toolName = originalTool?.name || "Tool";
							const toolInput = originalTool?.input || "";

							// Clean up the tool call from our tracking map
							if (entry.metadata.toolUseId) {
								this.toolCallsByToolUseId.delete(entry.metadata.toolUseId);
							}

							// Handle TaskCreate results: cache the task ID → subject mapping
							const baseToolName = toolName.replace("↪ ", "");
							if (baseToolName === "TaskCreate" && entry.metadata?.toolUseId) {
								const cachedSubject = this.taskSubjectsByToolUseId.get(
									entry.metadata.toolUseId,
								);
								if (cachedSubject) {
									// Parse task ID from result like "Task #1 created successfully: ..."
									const taskIdMatch = toolResult.content?.match(/Task #(\d+)/);
									if (taskIdMatch?.[1]) {
										this.taskSubjectsById.set(taskIdMatch[1], cachedSubject);
									}
									this.taskSubjectsByToolUseId.delete(
										entry.metadata.toolUseId!,
									);
								}
							}

							// Handle TaskUpdate/TaskGet results: post enriched thought with subject
							if (baseToolName === "TaskUpdate" || baseToolName === "TaskGet") {
								const formatter = session.agentRunner?.getFormatter();
								if (!formatter) {
									log.warn(`No formatter available for session ${sessionId}`);
									return;
								}

								// Try to enrich toolInput with subject from cache or result
								const enrichedInput = { ...toolInput };
								if (!enrichedInput.subject) {
									const taskId = enrichedInput.taskId || "";
									// First try: look up subject from our cache
									const cachedSubject = this.taskSubjectsById.get(taskId);
									if (cachedSubject) {
										enrichedInput.subject = cachedSubject;
									} else if (baseToolName === "TaskGet" && toolResult.content) {
										// Second try: parse subject from TaskGet result content
										// Format: "ID: 123\nSubject: Fix bug\nStatus: ..."
										const subjectMatch =
											toolResult.content.match(/^Subject:\s*(.+)$/m);
										if (subjectMatch?.[1]) {
											enrichedInput.subject = subjectMatch[1].trim();
											// Also cache it for future TaskUpdate calls
											if (taskId) {
												this.taskSubjectsById.set(
													taskId,
													enrichedInput.subject,
												);
											}
										}
									} else if (
										baseToolName === "TaskUpdate" &&
										toolResult.content
									) {
										// Try to parse subject from TaskUpdate result content
										// Format: "Updated task #3 subject" or may contain task details
										const subjectMatch =
											toolResult.content.match(/^Subject:\s*(.+)$/m);
										if (subjectMatch?.[1]) {
											enrichedInput.subject = subjectMatch[1].trim();
											if (taskId) {
												this.taskSubjectsById.set(
													taskId,
													enrichedInput.subject,
												);
											}
										}
									}
								}

								const formattedTask = formatter.formatTaskParameter(
									baseToolName,
									enrichedInput,
								);
								content = {
									type: "thought",
									body: formattedTask,
								};
								ephemeral = false;
								break;
							}

							// Skip creating activity for TodoWrite/write_todos results since they already created a non-ephemeral thought
							// Skip TaskCreate/TaskList results since they already created a non-ephemeral thought
							// Skip AskUserQuestion results since it's custom handled via Linear's select signal elicitation
							if (
								toolName === "TodoWrite" ||
								toolName === "↪ TodoWrite" ||
								toolName === "write_todos" ||
								toolName === "TaskCreate" ||
								toolName === "↪ TaskCreate" ||
								toolName === "TaskList" ||
								toolName === "↪ TaskList" ||
								toolName === "AskUserQuestion" ||
								toolName === "↪ AskUserQuestion"
							) {
								return;
							}

							// Get formatter from runner
							const formatter = session.agentRunner?.getFormatter();
							if (!formatter) {
								log.warn(`No formatter available`);
								return;
							}

							// Format parameter and result using runner's formatter
							const formattedParameter = formatter.formatToolParameter(
								toolName,
								toolInput,
							);
							const formattedResult = formatter.formatToolResult(
								toolName,
								toolInput,
								toolResult.content?.trim() || "",
								toolResult.isError,
							);

							// Format the action name (with description for Bash tool)
							const formattedAction = formatter.formatToolActionName(
								toolName,
								toolInput,
								toolResult.isError,
							);

							content = {
								type: "action",
								action: formattedAction,
								parameter: formattedParameter,
								result: formattedResult,
							};
						} else {
							return;
						}
					} else {
						return;
					}
					break;
				}
				case "assistant": {
					// Assistant messages can be thoughts or responses
					if (entry.metadata?.toolUseId) {
						const toolName = entry.metadata.toolName || "Tool";

						// Store tool information for later use in tool results
						if (entry.metadata.toolUseId) {
							// Check if this is a subtask with arrow prefix
							let storedName = toolName;
							if (entry.metadata?.parentToolUseId) {
								const activeTaskId = this.activeTasksBySession.get(sessionId);
								if (activeTaskId === entry.metadata?.parentToolUseId) {
									storedName = `↪ ${toolName}`;
								}
							}

							this.toolCallsByToolUseId.set(entry.metadata.toolUseId, {
								name: storedName,
								input: entry.metadata.toolInput || entry.content,
							});
						}

						// Skip AskUserQuestion tool - it's custom handled via Linear's select signal elicitation
						if (toolName === "AskUserQuestion") {
							return;
						}

						// Special handling for TodoWrite tool (Claude) and write_todos (Gemini) - treat as thought instead of action
						if (toolName === "TodoWrite" || toolName === "write_todos") {
							// Get formatter from runner
							const formatter = session.agentRunner?.getFormatter();
							if (!formatter) {
								log.warn(`No formatter available`);
								return;
							}

							const formattedTodos = formatter.formatTodoWriteParameter(
								entry.content,
							);
							content = {
								type: "thought",
								body: formattedTodos,
							};
							// TodoWrite/write_todos is not ephemeral
							ephemeral = false;
						} else if (toolName === "TaskCreate" || toolName === "TaskList") {
							// Get formatter from runner
							const formatter = session.agentRunner?.getFormatter();
							if (!formatter) {
								log.warn(`No formatter available for session ${sessionId}`);
								return;
							}

							// Special handling for Task tools - format as thought instead of action
							const toolInput = entry.metadata.toolInput || entry.content;
							const formattedTask = formatter.formatTaskParameter(
								toolName,
								toolInput,
							);
							content = {
								type: "thought",
								body: formattedTask,
							};
							// Task tools are not ephemeral
							ephemeral = false;

							// Cache TaskCreate subject by toolUseId so we can map it to task ID when result arrives
							if (
								toolName === "TaskCreate" &&
								toolInput?.subject &&
								entry.metadata.toolUseId
							) {
								this.taskSubjectsByToolUseId.set(
									entry.metadata.toolUseId,
									toolInput.subject,
								);
							}
						} else if (toolName === "TaskUpdate" || toolName === "TaskGet") {
							// Skip posting at tool_use time — defer to tool_result time
							// so we can enrich with subject from result or cache
							return;
						} else if (toolName === "Task") {
							// Get formatter from runner
							const formatter = session.agentRunner?.getFormatter();
							if (!formatter) {
								log.warn(`No formatter available`);
								return;
							}

							// Special handling for Task tool - add start marker and track active task
							const toolInput = entry.metadata.toolInput || entry.content;
							const formattedParameter = formatter.formatToolParameter(
								toolName,
								toolInput,
							);
							const displayName = toolName;

							// Track this as the active Task for this session
							if (entry.metadata?.toolUseId) {
								this.activeTasksBySession.set(
									sessionId,
									entry.metadata.toolUseId,
								);
							}

							content = {
								type: "action",
								action: displayName,
								parameter: formattedParameter,
								// result will be added later when we get tool result
							};
							// Task is not ephemeral
							ephemeral = false;
						} else {
							// Get formatter from runner
							const formatter = session.agentRunner?.getFormatter();
							if (!formatter) {
								log.warn(`No formatter available`);
								return;
							}

							// Other tools - check if they're within an active Task
							const toolInput = entry.metadata.toolInput || entry.content;
							let displayName = toolName;

							if (entry.metadata?.parentToolUseId) {
								const activeTaskId = this.activeTasksBySession.get(sessionId);
								if (activeTaskId === entry.metadata?.parentToolUseId) {
									displayName = `↪ ${toolName}`;
								}
							}

							const formattedParameter = formatter.formatToolParameter(
								displayName,
								toolInput,
							);

							content = {
								type: "action",
								action: displayName,
								parameter: formattedParameter,
								// result will be added later when we get tool result
							};
							// Standard tool calls are ephemeral
							ephemeral = true;
						}
					} else if (entry.metadata?.sdkError) {
						// Assistant message with SDK error (e.g., rate_limit, billing_error)
						// Create an error type so it's visible to users (not just a thought)
						// Per CYPACK-719: usage limits should trigger "error" type activity
						content = {
							type: "error",
							body: entry.content,
						};
					} else {
						// Regular assistant message - create a thought
						content = {
							type: "thought",
							body: entry.content,
						};
					}
					break;
				}

				case "system":
					// System messages are thoughts
					content = {
						type: "thought",
						body: entry.content,
					};
					break;

				case "result":
					// Result messages can be responses or errors
					if (entry.metadata?.isError) {
						content = {
							type: "error",
							body: entry.content,
						};
					} else {
						content = {
							type: "response",
							body: entry.content,
						};
					}
					break;

				default:
					// Default to thought
					content = {
						type: "thought",
						body: entry.content,
					};
			}

			// Ensure we have an external session ID for activity posting
			if (!session.externalSessionId) {
				log.debug(
					`Skipping activity sync - no external session ID (platform: ${session.issueContext?.trackerId || "unknown"})`,
				);
				return;
			}

			const options: ActivityPostOptions = {};
			if (ephemeral) {
				options.ephemeral = true;
			}

			const activitySink = this.getActivitySink(sessionId);
			if (!activitySink) {
				log.debug(
					`Skipping activity sync - no activity sink registered for session`,
				);
				return;
			}

			const result = await activitySink.postActivity(
				session.externalSessionId,
				content,
				options,
			);

			if (result.activityId) {
				entry.linearAgentActivityId = result.activityId;
				if (entry.type === "result") {
					log.info(
						`Result message emitted to Linear (activity ${entry.linearAgentActivityId})`,
					);
				} else {
					log.debug(
						`Created ${content.type} activity ${entry.linearAgentActivityId}`,
					);
				}
			}
		} catch (error) {
			log.error(`Failed to sync entry to activity sink:`, error);
		}
	}

	/**
	 * Get session by ID
	 */
	getSession(sessionId: string): CyrusAgentSession | undefined {
		return this.sessions.get(sessionId);
	}

	/**
	 * Get session entries by session ID
	 */
	getSessionEntries(sessionId: string): CyrusAgentSessionEntry[] {
		return this.entries.get(sessionId) || [];
	}

	/**
	 * Get all active sessions
	 */
	getActiveSessions(): CyrusAgentSession[] {
		return Array.from(this.sessions.values()).filter(
			(session) => session.status === AgentSessionStatus.Active,
		);
	}

	/**
	 * Session IDs the device is still responsible for terminal-signalling, sent
	 * to the router in the hello frame so it can reclaim issue locks for every
	 * session NOT in this set.
	 *
	 * A session qualifies if it either has a live runner OR is not yet in a
	 * terminal status (complete/error). The two clauses cover two distinct
	 * leak-prone cases:
	 *
	 * - **Runner attached, status already terminal.** `completeSession` flips
	 *   the status to complete/error *before* deferring the terminal signal when
	 *   the runner still has pending work (a scheduled wakeup / background task).
	 *   The lock must stay held through that deferral, so a session with a live
	 *   runner is always declared even though its status reads terminal — else a
	 *   mere reconnect mid-deferral would wrongly reclaim a still-working
	 *   session's lock.
	 *
	 * - **Non-terminal, no runner.** A session interrupted by a host restart
	 *   restores as active/pending with no runner (runners are never
	 *   serialized); `reconcileInterruptedSessions` will drive it terminal and
	 *   release the lock, so keep declaring it until then.
	 *
	 * Everything else — a terminal session with no runner — is a session the
	 * device will never send a terminal frame for: either it already did (lock
	 * long gone, so declaring it is moot) or its deferred wakeup died with a
	 * previous process (the lock leaked). Omitting it lets the router reclaim
	 * that stranded lock. Sessions the device lost entirely (e.g. a corrupt
	 * state file) are absent here too, which is likewise correct to reclaim.
	 */
	getLiveSessionIds(): string[] {
		const isTerminal = (status: CyrusAgentSession["status"]): boolean =>
			status === AgentSessionStatus.Complete ||
			status === AgentSessionStatus.Error;
		return Array.from(this.sessions.entries())
			.filter(([, s]) => s.agentRunner || !isTerminal(s.status))
			.map(([id]) => id);
	}

	/**
	 * Reports sessions that a host crash interrupted, so they stop looking alive.
	 *
	 * A SIGKILL (OOM kill, `pm2 delete`, power loss) gives no in-process handler a
	 * chance to run: no result message, so no {@link completeSession}, so neither
	 * a terminal activity nor a "sessionTerminal" emit. Linear derives session
	 * state from the last emitted activity, so the session stays `active`
	 * ("Working...") indefinitely — indistinguishable from one still running — and
	 * in router mode the issue lock and session affinity stay held forever,
	 * because the router only releases them when the device says the session
	 * ended.
	 *
	 * Runners are runtime-only and never serialized, so after
	 * {@link restoreState} a persisted session that is still `active`/`pending`
	 * yet has no runner is exactly a session the previous process was running when
	 * it died. A clean shutdown does not produce these: it stops each runner,
	 * which yields a result and drives the session to `complete`/`error` first.
	 *
	 * `awaitingInput` is deliberately excluded — that is a legitimate paused state
	 * waiting on a user answer, and the next prompt resumes it.
	 *
	 * MUST run once at startup, after {@link restoreState} and before any new
	 * runner is attached. Both the activity post and the terminal signal are
	 * durable across a router disconnect, so it is safe to call before the device
	 * has connected.
	 *
	 * @returns the ids of the sessions it reconciled
	 */
	async reconcileInterruptedSessions(): Promise<string[]> {
		const interrupted: string[] = [];

		for (const [sessionId, session] of this.sessions) {
			if (session.agentRunner) continue;
			if (
				session.status !== AgentSessionStatus.Active &&
				session.status !== AgentSessionStatus.Pending
			) {
				continue;
			}
			// A session that already signalled a terminal state finished — and
			// reported finishing — before the host went away, whatever its status
			// field says. `status` alone is not enough: the pending-work deferral
			// flips it to `complete` while the session keeps running, and a
			// restore-time rewrite (e.g. the floor bundle's runner-id
			// invalidation) can leave it looking active. Treating one of these as
			// interrupted replays a terminal `error` that the timeline has already
			// moved past.
			if (
				session.terminalState ||
				this.terminalEmittedSessions.has(sessionId)
			) {
				continue;
			}

			interrupted.push(sessionId);
			const log = this.sessionLog(sessionId);
			await this.updateSessionStatus(sessionId, AgentSessionStatus.Error);

			// Best-effort: a failed post must not stop the terminal signal, which is
			// the only thing that releases the router's issue lock and affinity.
			try {
				if (this.isOpenCodeSession(sessionId)) {
					await this.postOpenCodeLifecycleNotice(
						sessionId,
						"failed",
						"OpenCode ended unexpectedly.",
					);
				}
				await this.createErrorActivity(
					sessionId,
					"Session interrupted — the agent host restarted before this session finished. Send a new message to resume it.",
				);
			} catch (err) {
				log.error("Failed to post interrupted-session activity:", err);
			}

			await this.emitTerminalOnce(sessionId, "error");
			log.info("Reconciled session interrupted by a host restart");
		}

		return interrupted;
	}

	/**
	 * Add or update agent runner for a session
	 */
	addAgentRunner(sessionId: string, agentRunner: IAgentRunner): void {
		const log = this.sessionLog(sessionId);
		const session = this.sessions.get(sessionId);
		if (!session) {
			log.warn(`No session found`);
			return;
		}

		// Revive a session that already emitted its terminal signal. Two paths
		// reach here with a spent one-shot: a floor-restored session, and one
		// that {@link reconcileInterruptedSessions} marked `error` at startup
		// before its queued prompt re-attached a runner. Attaching a live runner
		// means the session is running again, so:
		//   1. clear the one-shot — otherwise its next real completion's
		//      `emitTerminalOnce` is a no-op and the router's issue lock +
		//      affinity are never released (they leak until the event TTL sweep);
		//   2. lift a terminal status back to `active` so its activities post and
		//      Linear shows it working rather than stuck in the reconciled error.
		if (
			this.terminalEmittedSessions.delete(sessionId) ||
			session.terminalState
		) {
			if (
				session.status === AgentSessionStatus.Complete ||
				session.status === AgentSessionStatus.Error
			) {
				session.status = AgentSessionStatus.Active;
			}
			log.debug(`Revived terminal-emitted session on runner (re)attach`);
		}
		// The session has advanced past whatever terminal state it last reported,
		// so that state is now stale. Clearing it here is what makes terminal
		// reporting monotonic: reconciliation stops treating the session as
		// already-finished, and "sessionResumed" tells the router transport to drop
		// any still-unacked terminal frame for it rather than replaying that frame
		// mid-turn and stripping the affinity this turn's activities post under.
		session.terminalState = undefined;

		session.agentRunner = agentRunner;
		if (runnerTypeOf(agentRunner) === "opencode") {
			this.openCodeTurnBySession.set(
				sessionId,
				(this.openCodeTurnBySession.get(sessionId) ?? 0) + 1,
			);
			this.startOperatorHeartbeat(sessionId);
		}
		this.getObservabilityProjection(sessionId).recordRunner({ type: "alive" });
		session.updatedAt = Date.now();
		log.debug(`Added agent runner`);
		this.emit("sessionResumed", sessionId);
	}

	/**
	 *  Get all agent runners
	 */
	getAllAgentRunners(): IAgentRunner[] {
		return Array.from(this.sessions.values())
			.map((session) => session.agentRunner)
			.filter((runner): runner is IAgentRunner => runner !== undefined);
	}

	/**
	 * Resolve the issue ID from a session, checking issueContext first then deprecated issueId.
	 */
	private getSessionIssueId(session: CyrusAgentSession): string | undefined {
		return session.issueContext?.issueId ?? session.issueId;
	}

	/**
	 * Get all agent runners for a specific issue
	 */
	getAgentRunnersForIssue(issueId: string): IAgentRunner[] {
		return Array.from(this.sessions.values())
			.filter((session) => this.getSessionIssueId(session) === issueId)
			.map((session) => session.agentRunner)
			.filter((runner): runner is IAgentRunner => runner !== undefined);
	}

	/**
	 * Get sessions by issue ID
	 */
	getSessionsByIssueId(issueId: string): CyrusAgentSession[] {
		return Array.from(this.sessions.values()).filter(
			(session) => this.getSessionIssueId(session) === issueId,
		);
	}

	/**
	 * Get active sessions by issue ID
	 */
	getActiveSessionsByIssueId(issueId: string): CyrusAgentSession[] {
		return Array.from(this.sessions.values()).filter(
			(session) =>
				this.getSessionIssueId(session) === issueId &&
				session.status === AgentSessionStatus.Active,
		);
	}

	/**
	 * Get active sessions where the issue's branch name matches the given branch.
	 * Useful for detecting when multiple sessions share the same worktree.
	 */
	getActiveSessionsByBranchName(branchName: string): CyrusAgentSession[] {
		return Array.from(this.sessions.values()).filter(
			(session) =>
				session.status === AgentSessionStatus.Active &&
				session.issue?.branchName === branchName,
		);
	}

	/**
	 * Get active sessions tracking a given base branch for a specific repository.
	 * Used by GitHub push webhook handling to notify agents when their base branch receives new commits.
	 */
	getSessionsByBaseBranch(
		baseBranchName: string,
		repositoryId: string,
	): CyrusAgentSession[] {
		return Array.from(this.sessions.values()).filter(
			(session) =>
				session.status === AgentSessionStatus.Active &&
				session.repositories.some(
					(r) =>
						r.repositoryId === repositoryId &&
						r.baseBranchName === baseBranchName,
				),
		);
	}

	/**
	 * Find an active multi-repo session that includes the given repository.
	 * Used by GitHub webhook handling to resolve the correct sub-worktree
	 * when a @ mention targets a specific repo within a multi-repo workspace.
	 */
	getActiveMultiRepoSessionForRepository(
		repositoryId: string,
	): CyrusAgentSession | null {
		for (const session of this.sessions.values()) {
			if (session.status !== AgentSessionStatus.Active) continue;
			if (!session.workspace.repoPaths) continue; // not multi-repo
			const matchesRepo = session.repositories.some(
				(r) => r.repositoryId === repositoryId,
			);
			if (matchesRepo) {
				return session;
			}
		}
		return null;
	}

	/**
	 * Get all sessions
	 */
	getAllSessions(): CyrusAgentSession[] {
		return Array.from(this.sessions.values());
	}

	/**
	 * Get agent runner for a specific session
	 */
	getAgentRunner(sessionId: string): IAgentRunner | undefined {
		const session = this.sessions.get(sessionId);
		return session?.agentRunner;
	}

	/**
	 * Check if an agent runner exists for a session
	 */
	hasAgentRunner(sessionId: string): boolean {
		const session = this.sessions.get(sessionId);
		return session?.agentRunner !== undefined;
	}

	/**
	 * Post an activity to the activity sink for a session.
	 * Consolidates session lookup, externalSessionId guard, try/catch, and logging.
	 *
	 * @returns The activity ID when resolved, `null` otherwise.
	 */
	private async postActivity(
		sessionId: string,
		input: {
			content: any;
			ephemeral?: boolean;
			signal?: ActivitySignal;
			signalMetadata?: Record<string, unknown>;
		},
		label: string,
	): Promise<string | null> {
		const log = this.sessionLog(sessionId);
		const session = this.sessions.get(sessionId);

		if (!session?.externalSessionId) {
			log.debug(
				`Skipping ${label} - no external session ID (platform: ${session?.issueContext?.trackerId || "unknown"})`,
			);
			return null;
		}

		try {
			const options: ActivityPostOptions = {};
			if (input.ephemeral !== undefined) {
				options.ephemeral = input.ephemeral;
			}
			if (input.signal) {
				options.signal = input.signal;
			}
			if (input.signalMetadata) {
				options.signalMetadata = input.signalMetadata;
			}

			const activitySink = this.getActivitySink(sessionId);
			if (!activitySink) {
				log.debug(
					`Skipping ${label} - no activity sink registered for session`,
				);
				return null;
			}

			const result = await activitySink.postActivity(
				session.externalSessionId,
				input.content,
				options,
			);

			if (result.activityId) {
				log.debug(`Created ${label} activity ${result.activityId}`);
				return result.activityId;
			}
			log.debug(`Created ${label}`);
			return null;
		} catch (error) {
			log.error(`Error creating ${label}:`, error);
			return null;
		}
	}

	private startOperatorHeartbeat(sessionId: string): void {
		if (this.operatorHeartbeatTimers.has(sessionId)) return;
		const timer = setInterval(() => {
			const session = this.sessions.get(sessionId);
			if (!session || session.status !== AgentSessionStatus.Active) {
				this.stopOperatorHeartbeat(sessionId);
				return;
			}
			const now = Date.now();
			const lastProgress = this.lastOperatorProgressAt.get(sessionId) ?? now;
			if (now - lastProgress < OPENCODE_HEARTBEAT_INTERVAL_MS) return;
			this.lastOperatorProgressAt.set(sessionId, now);
			void this.reportCurrentRunnerHealth(sessionId, now);
			const rolling = this.getObservabilityProjection(sessionId).status(now);
			const elapsedMinutes = Math.floor(rolling.elapsedMs / 60_000);
			const elapsedSeconds = Math.floor((rolling.elapsedMs % 60_000) / 1_000);
			const elapsed = `${elapsedMinutes}m ${elapsedSeconds}s`;
			const progress = rolling.todo
				? ` · ${rolling.todo.completed}/${rolling.todo.total} done`
				: "";
			const attention =
				rolling.attention.length > 0
					? ` · ${rolling.attention.join(", ")}`
					: "";
			void this.postActivity(
				sessionId,
				{
					ephemeral: true,
					content: {
						type: "thought",
						body: `OpenCode active · ${rolling.phase ?? "working"} · ${elapsed}${progress}${attention}`,
					},
				},
				"CEO status",
			);
		}, OPENCODE_HEARTBEAT_INTERVAL_MS);
		// A feedback timer must never keep the worker alive by itself.
		timer.unref?.();
		this.operatorHeartbeatTimers.set(sessionId, timer);
		this.lastOperatorProgressAt.set(sessionId, Date.now());
	}

	private stopOperatorHeartbeat(sessionId: string): void {
		const timer = this.operatorHeartbeatTimers.get(sessionId);
		if (timer) clearInterval(timer);
		this.operatorHeartbeatTimers.delete(sessionId);
	}

	private async postOpenCodeCheckpoint(
		sessionId: string,
		key: string,
		body: string,
	): Promise<void> {
		const now = Date.now();
		const checkpoints =
			this.operatorFeedbackBySession.get(sessionId) ??
			new Map<string, number>();
		const previousAt = checkpoints.get(key);
		if (
			previousAt !== undefined &&
			now - previousAt < OPENCODE_CHECKPOINT_TTL_MS
		)
			return;
		checkpoints.set(key, now);
		this.operatorFeedbackBySession.set(sessionId, checkpoints);
		this.lastOperatorProgressAt.set(sessionId, now);
		await this.postActivity(
			sessionId,
			{ content: { type: "thought", body } },
			"OpenCode checkpoint",
		);
	}

	/**
	 * Emits a deduplicated Linear-facing health activity. This is intentionally
	 * observation-only: neither a stale stream nor an optional plugin failure
	 * stops, restarts, or otherwise mutates the attached runner.
	 */
	async reportRunnerHealth(
		sessionId: string,
		signal: RunnerHealthSignal,
	): Promise<void> {
		const projection = this.getObservabilityProjection(sessionId);
		projection.recordRunner(signal);
		const activity = projection.takeHealthActivity(
			signal.type === "snapshot"
				? (signal.snapshot.lastEventAt ?? Date.now())
				: (signal.at ?? Date.now()),
		);
		if (!activity) return;
		await this.postActivity(sessionId, { content: activity }, "runner health");
	}

	private async reportCurrentRunnerHealth(
		sessionId: string,
		now: number,
	): Promise<void> {
		const runner = this.sessions.get(sessionId)?.agentRunner as
			| (IAgentRunner & {
					getHealthSnapshot?: () => RunnerHealthSnapshot;
			  })
			| undefined;
		const snapshot = runner?.getHealthSnapshot?.();
		await this.reportRunnerHealth(
			sessionId,
			snapshot ? { type: "snapshot", snapshot } : { type: "alive", at: now },
		);
	}

	/**
	 * Generic ingress for optional plugin phase/tool/gate/review/final signals.
	 * Plugin availability contributes to the same health sentence as runner
	 * liveness; it is never a lifecycle command.
	 */
	async reportOptionalPluginTelemetry(
		sessionId: string,
		signal: OptionalPluginTelemetrySignal,
	): Promise<void> {
		const projection = this.getObservabilityProjection(sessionId);
		const semanticActivity = projection.recordPlugin(signal);
		if (semanticActivity) {
			await this.postActivity(
				sessionId,
				{ content: semanticActivity },
				"optional plugin telemetry",
			);
		}
		const healthActivity = projection.takeHealthActivity();
		if (healthActivity) {
			await this.postActivity(
				sessionId,
				{ content: healthActivity },
				"runner health",
			);
		}
	}

	private getObservabilityProjection(
		sessionId: string,
	): SessionObservabilityProjection {
		let projection = this.observabilityBySession.get(sessionId);
		if (!projection) {
			projection = new SessionObservabilityProjection({
				staleOutputAfterMs: OPENCODE_HEARTBEAT_INTERVAL_MS,
			});
			this.observabilityBySession.set(sessionId, projection);
		}
		return projection;
	}

	/**
	 * Report that a Linear follow-up will be delivered after the live OpenCode
	 * turn ends. EdgeWorker owns the queue; this manager owns the safe, bounded
	 * Linear-facing lifecycle notice.
	 */
	async reportOpenCodeFollowUpQueued(sessionId: string): Promise<void> {
		if (!this.isOpenCodeSession(sessionId)) return;
		await this.postOpenCodeLifecycleNotice(
			sessionId,
			"follow-up-queued",
			"A follow-up is queued and will be handled after this OpenCode turn.",
		);
	}

	private isOpenCodeSession(sessionId: string): boolean {
		const session = this.sessions.get(sessionId);
		return Boolean(
			session?.opencodeSessionId ||
				(session?.agentRunner &&
					runnerTypeOf(session.agentRunner) === "opencode"),
		);
	}

	private async postOpenCodeLifecycleNotice(
		sessionId: string,
		phase: string,
		body: string,
		type: "thought" | "error" = "thought",
	): Promise<void> {
		const turn = this.openCodeTurnBySession.get(sessionId) ?? 0;
		const key = `${sessionId}:${turn}:${phase}`;
		if (this.openCodeLifecycleNotices.has(key)) return;
		this.openCodeLifecycleNotices.add(key);
		await this.postActivity(
			sessionId,
			{ content: { type, body } },
			"OpenCode lifecycle",
		);
	}

	/**
	 * Create a thought activity
	 */
	async createThoughtActivity(sessionId: string, body: string): Promise<void> {
		await this.postActivity(
			sessionId,
			{ content: { type: "thought", body } },
			"thought",
		);
	}

	/**
	 * Create an action activity
	 */
	async createActionActivity(
		sessionId: string,
		action: string,
		parameter: string,
		result?: string,
	): Promise<void> {
		const content: any = { type: "action", action, parameter };
		if (result !== undefined) {
			content.result = result;
		}
		await this.postActivity(sessionId, { content }, "action");
	}

	/**
	 * Create a response activity
	 */
	async createResponseActivity(sessionId: string, body: string): Promise<void> {
		await this.postActivity(
			sessionId,
			{ content: { type: "response", body } },
			"response",
		);
	}

	/**
	 * Create an error activity
	 */
	async createErrorActivity(sessionId: string, body: string): Promise<void> {
		await this.postActivity(
			sessionId,
			{ content: { type: "error", body } },
			"error",
		);
	}

	/**
	 * Create an elicitation activity
	 */
	async createElicitationActivity(
		sessionId: string,
		body: string,
	): Promise<void> {
		await this.postActivity(
			sessionId,
			{ content: { type: "elicitation", body } },
			"elicitation",
		);
	}

	/**
	 * Create an approval elicitation activity with auth signal
	 */
	async createApprovalElicitation(
		sessionId: string,
		body: string,
		approvalUrl: string,
	): Promise<void> {
		await this.postActivity(
			sessionId,
			{
				content: { type: "elicitation", body },
				signal: "auth",
				signalMetadata: { url: approvalUrl },
			},
			"approval elicitation",
		);
	}

	/**
	 * Remove a session and all associated tracking state.
	 * Use for immediate cleanup when a session is permanently done
	 * (e.g., issue moved to terminal state).
	 */
	removeSession(sessionId: string): void {
		const log = this.sessionLog(sessionId);
		this.sessions.delete(sessionId);
		this.entries.delete(sessionId);
		this.activitySinks.delete(sessionId);
		this.activeTasksBySession.delete(sessionId);
		this.activeStatusActivitiesBySession.delete(sessionId);
		this.stopRequestedSessions.delete(sessionId);
		this.terminalEmittedSessions.delete(sessionId);
		this.lastAssistantBodyBySession.delete(sessionId);
		this.bufferedAssistantEntryBySession.delete(sessionId);
		this.messageProcessingQueues.delete(sessionId);
		this.stopOperatorHeartbeat(sessionId);
		this.operatorFeedbackBySession.delete(sessionId);
		this.lastOperatorProgressAt.delete(sessionId);
		this.openCodeTurnBySession.delete(sessionId);
		this.observabilityBySession.delete(sessionId);
		for (const key of this.openCodeLifecycleNotices) {
			if (key.startsWith(`${sessionId}:`)) {
				this.openCodeLifecycleNotices.delete(key);
			}
		}
		log.debug("Removed session");
	}

	/**
	 * Clear completed sessions older than specified time
	 */
	cleanup(olderThanMs: number = 24 * 60 * 60 * 1000): void {
		const cutoff = Date.now() - olderThanMs;

		for (const [sessionId, session] of this.sessions.entries()) {
			if (
				(session.status === "complete" || session.status === "error") &&
				session.updatedAt < cutoff
			) {
				const log = this.sessionLog(sessionId);
				this.sessions.delete(sessionId);
				this.entries.delete(sessionId);
				log.debug(`Cleaned up session`);
			}
		}
	}

	/**
	 * Serialize Agent Session state for persistence
	 */
	serializeState(): {
		sessions: Record<string, SerializedCyrusAgentSession>;
		entries: Record<string, SerializedCyrusAgentSessionEntry[]>;
	} {
		const sessions: Record<string, SerializedCyrusAgentSession> = {};
		const entries: Record<string, SerializedCyrusAgentSessionEntry[]> = {};

		// Serialize sessions
		for (const [sessionId, session] of this.sessions.entries()) {
			// Exclude agentRunner from serialization as it's not serializable
			const { agentRunner: _agentRunner, ...serializableSession } = session;
			sessions[sessionId] = serializableSession;
		}

		// Serialize entries
		for (const [sessionId, sessionEntries] of this.entries.entries()) {
			entries[sessionId] = sessionEntries.map((entry) => ({
				...entry,
			}));
		}

		return { sessions, entries };
	}

	/**
	 * Restore Agent Session state from serialized data
	 */
	restoreState(
		serializedSessions: Record<string, SerializedCyrusAgentSession>,
		serializedEntries: Record<string, SerializedCyrusAgentSessionEntry[]>,
	): void {
		// Clear existing state
		this.sessions.clear();
		this.entries.clear();
		this.terminalEmittedSessions.clear();

		// Restore sessions (migrate old sessions without repositories field)
		for (const [sessionId, sessionData] of Object.entries(serializedSessions)) {
			const session: CyrusAgentSession = {
				...sessionData,
				repositories: sessionData.repositories ?? [],
			};
			this.sessions.set(sessionId, session);
			// Restore the terminal one-shot alongside the session. The signal was
			// already delivered before the host went away (the router released the
			// lock, the floor took its bundle), so re-emitting it would be a stale
			// replay; `addAgentRunner` re-arms it when the session next runs.
			if (session.terminalState) {
				this.terminalEmittedSessions.add(sessionId);
			}
		}

		// Restore entries
		for (const [sessionId, entriesData] of Object.entries(serializedEntries)) {
			const sessionEntries: CyrusAgentSessionEntry[] = entriesData.map(
				(entryData) => ({
					...entryData,
				}),
			);
			this.entries.set(sessionId, sessionEntries);
		}

		this.logger.debug(
			`Restored ${this.sessions.size} sessions, ${Object.keys(serializedEntries).length} entry collections`,
		);
	}

	/**
	 * Post a thought about the model being used
	 */
	private async postModelNotificationThought(
		sessionId: string,
		model: string,
	): Promise<void> {
		const displayModel = this.formatModelNotification(sessionId, model);
		await this.postActivity(
			sessionId,
			{ content: { type: "thought", body: `Using model: ${displayModel}` } },
			"model notification",
		);
	}

	private formatModelNotification(sessionId: string, model: string): string {
		const runnerType = this.getSessionRunnerType(sessionId);
		if (model.startsWith(`${runnerType}/`)) {
			return model;
		}
		return `${runnerType}/${model}`;
	}

	private getSessionRunnerType(sessionId: string): RunnerType {
		const runner = this.sessions.get(sessionId)?.agentRunner;
		// A session with no runner attached reads as "claude", the default runner,
		// because this feeds a model-name prefix that must always produce one.
		// `getRunFacts` deliberately does NOT share that fallback — see there.
		return runner ? runnerTypeOf(runner) : "claude";
	}

	/**
	 * Post an ephemeral "Analyzing your request..." thought and return the activity ID
	 */
	async postAnalyzingThought(sessionId: string): Promise<string | null> {
		return this.postActivity(
			sessionId,
			{
				content: { type: "thought", body: "Analyzing your request…" },
				ephemeral: true,
			},
			"analyzing thought",
		);
	}

	/**
	 * Handle status messages (compacting, etc.)
	 */
	private async handleStatusMessage(
		sessionId: string,
		message: SDKStatusMessage,
	): Promise<void> {
		const session = this.sessions.get(sessionId);
		if (!session?.externalSessionId) {
			const log = this.sessionLog(sessionId);
			log.debug(
				`Skipping status message - no external session ID (platform: ${session?.issueContext?.trackerId || "unknown"})`,
			);
			return;
		}

		if (message.status === "compacting") {
			const activityId = await this.postActivity(
				sessionId,
				{
					content: {
						type: "thought",
						body: "Compacting conversation history…",
					},
					ephemeral: true,
				},
				"compacting status",
			);
			if (activityId) {
				this.activeStatusActivitiesBySession.set(sessionId, activityId);
			}
		} else if (message.status === null) {
			// Clear the status - post a non-ephemeral thought to replace the ephemeral one
			await this.postActivity(
				sessionId,
				{
					content: { type: "thought", body: "Conversation history compacted" },
					ephemeral: false,
				},
				"status clear",
			);
			// Clean up the stored activity ID regardless — stale IDs do no harm
			this.activeStatusActivitiesBySession.delete(sessionId);
		}
	}
}
