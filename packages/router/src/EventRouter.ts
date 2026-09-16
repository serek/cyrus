import {
	type AgentEvent,
	type AgentSessionCreatedWebhook,
	cyrusAttributes,
	type ILogger,
	isAgentSessionCreatedWebhook,
	isAgentSessionPromptedWebhook,
	isIssueDeletedWebhook,
	isIssueNewCommentWebhook,
	isIssueStateChangeWebhook,
	type LogEventAttributes,
	type RunAttribution,
	type Webhook,
} from "cyrus-core";
import {
	cyrusSpanAttributes,
	injectTraceContext,
	SpanKind,
	withSpan,
	withSpanSync,
} from "cyrus-otel-traces";
import {
	type SessionStateFrame,
	WAIT_REASON_ELICITATION,
} from "cyrus-router-protocol";
import {
	type ContainerTargetService,
	InvalidIssueKeyError,
} from "./ContainerTargets.js";
import type { DeviceGateway } from "./DeviceGateway.js";
import type { DevcontainerImageService } from "./devcontainer/DevcontainerImageService.js";
import { webhookIdempotencyKey } from "./idempotency.js";
import {
	DEVCONTAINER_BUILD_FAILED_MESSAGE,
	DEVCONTAINER_BUILDING_MESSAGE,
	DEVCONTAINER_MULTI_REPO_MESSAGE,
	DEVCONTAINER_READY_MESSAGE,
	expiredMessage,
	fillTemplate,
	INVALID_ISSUE_KEY_MESSAGE,
	ISSUE_LOCKED_BY_OTHER_MESSAGE,
	ISSUE_LOCKED_MESSAGE,
	NO_REPOSITORIES_MESSAGE,
	ORPHANED_LOCK_RECLAIMED_MESSAGE,
	offlineReleaseMessage,
	offlineWaitingMessage,
	PROMPT_REJECTION_MESSAGE,
	PROMPT_UNROUTABLE_MESSAGE,
	REPOSITORY_SELECTION_EXPIRED_MESSAGE,
	REPOSITORY_SELECTION_PROMPT,
	UNENROLLED_CREATOR_MESSAGE,
} from "./messages.js";
import {
	BASE_BRANCH_RE,
	type RegisteredRepository,
	type RepositoryRegistry,
} from "./RepositoryRegistry.js";
import type {
	RepositoryDecision,
	RepositoryResolver,
} from "./RepositoryResolver.js";
import type { AgentRunWait, RouterStore } from "./RouterStore.js";
import {
	emitRoutingRejection,
	emitRunEvent,
	emitSessionOwnershipRefusal,
	RUN_EVENTS,
	type RunEventName,
	type RunUnknownReason,
	routingAttribution,
	runAttribution,
	type SessionOwnershipRefusal,
} from "./RouterTelemetry.js";
import {
	emitSandboxEvent,
	SANDBOX_EVENTS,
	type SandboxEventName,
} from "./SandboxTelemetry.js";
import type {
	TerminalTeardown,
	TerminalTeardownAction,
} from "./TerminalTeardown.js";
import { ROUTER_SPANS, routerTracer } from "./telemetry/tracing.js";

/**
 * `agentSessionCreated` and `agentSessionPrompted` webhooks are the same
 * underlying `AgentSessionEventWebhookPayload`; the type guards only differ by
 * `action`. We route both through helpers typed with this alias.
 */
type SessionEvent = AgentSessionCreatedWebhook;

/** Shape we persist as `creator_json` in session affinity (a serialized creator). */
interface StoredCreator {
	id?: string;
	email?: string;
	name?: string;
}

/** Resolved routing target for a created event. */
interface ResolvedTarget {
	deviceId: number;
	/** Email used in offline/expiry notices. */
	email: string;
	/**
	 * "device" for a physical enrolled device, "container" for a per-issue
	 * ephemeral container device. Determines whether an offline target is an
	 * outage (post the waiting notice) or an expected cold start (boot it).
	 */
	kind: "device" | "container";
	/** Set for container targets: the issue key the container was minted for. */
	issueKey?: string;
}

export interface EventRouterOptions {
	store: RouterStore;
	gateway: Pick<DeviceGateway, "isOnline" | "deliverPending">;
	postActivity: (
		workspaceId: string,
		agentSessionId: string,
		body: string,
	) => Promise<void>;
	/**
	 * Moves an issue into its team's first `started` state, resolving with the
	 * state's name (or `undefined` when it was already started). Only the router
	 * holds a Linear token, so only the router can do this — see
	 * {@link LinearExecutor.moveIssueToStartedState}. Optional: omitting it
	 * disables promotion (tests that don't exercise it).
	 */
	moveIssueToStartedState?: (
		workspaceId: string,
		issueId: string,
	) => Promise<string | undefined>;
	/**
	 * Reads the Linear team and project an issue belongs to, for a run's routing
	 * snapshot — see {@link LinearExecutor.fetchRoutingContext}.
	 *
	 * Needed because the agent-session webhook carries the issue's team but NOT
	 * its project, so without this the project dimension of the snapshot could
	 * never be filled and the filter it exists for would match nothing.
	 *
	 * Called at most once per run and never on the delivery path. Optional:
	 * omitting it leaves the snapshot with whatever the webhook carried, which is
	 * a missing filter dimension rather than a broken route.
	 */
	fetchRoutingContext?: (
		workspaceId: string,
		issueId: string,
	) => Promise<
		| {
				linearTeamId?: string;
				linearTeamName?: string;
				linearProjectId?: string;
				linearProjectName?: string;
		  }
		| undefined
	>;
	/**
	 * Routes container-executor users to per-issue ephemeral container
	 * devices instead of a physical enrolled device. Optional: omitting it
	 * keeps every user on the physical-device path (today's behavior).
	 */
	containerTargets?: ContainerTargetService;
	/**
	 * Decides which repositories an issue routes to, before any container is
	 * created. Optional: omitting it keeps every container booting with the whole
	 * configured repository list (pre-registry behaviour).
	 */
	repositoryResolver?: RepositoryResolver;
	/** `LinearExecutor.postRepositorySelection`. Required alongside the resolver. */
	postRepositorySelection?: (
		workspaceId: string,
		agentSessionId: string,
		body: string,
		options: string[],
	) => Promise<void>;
	terminalTeardown?: TerminalTeardown;
	/**
	 * Builds and pins per-repository devcontainer images (NOR-309). Optional:
	 * omitting it boots every container from the default worker image, which is
	 * what every deployment did before this existed.
	 */
	devcontainers?: DevcontainerImageService;
	/** The live repository registry, read to find an issue's primary repository. */
	registry?: RepositoryRegistry;
	config: {
		eventTtlMs: number;
		issueLock: boolean;
		creatorOnlyPrompting: boolean;
		/**
		 * How long a claimed webhook idempotency key is remembered before
		 * {@link EventRouter.sweepExpired} discards it. Defaults to
		 * {@link DEFAULT_WEBHOOK_CLAIM_RETENTION_MS}.
		 */
		webhookClaimRetentionMs?: number;
		/** An undeclared affinity row younger than this is never reclaimed — it may
		 *  belong to a session the device was routed but has not started tracking. */
		affinityGraceMs: number;
	};
	logger: ILogger;
	/** Injectable clock (default `Date.now`) so TTL behavior is deterministic in tests. */
	now?: () => number;
}

const DEFAULT_EMAIL = "the delegating user";

/**
 * 5 minutes — how long a device may keep making session-scoped RPCs for a
 * session after it reported that session terminal.
 *
 * Sized to cover the tail of a finishing run, not to keep a dead session alive:
 * the worker posts its closing summary and then sends the terminal frame, so
 * the posts that need to survive this window are seconds behind it, and the
 * observed straggler bursts that motivated it were minutes long at most within
 * the part worth keeping. A session still emitting an hour after it completed
 * (NOR-405 saw one at 88 minutes) is the bug itself, and should still be
 * refused — loudly.
 */
export const TERMINAL_OWNERSHIP_GRACE_MS = 5 * 60 * 1000;

/**
 * 24 hours — how long a device may keep posting for a session it has parked.
 *
 * Much longer than the terminal window because a park is not an ending: the
 * session is blocked on a human answering, which can take a working day, and
 * for that whole time it is legitimately the device's session. On a deployment
 * with issue locking on, the lock already grants exactly this for an unbounded
 * period, so this is strictly tighter than the status quo; with issue locking
 * off it is the ONLY thing covering the park window, and without it a parked
 * session's posts are still dropped.
 *
 * An unparked session drops back to affinity immediately (the `active` branch
 * clears this), and a park that ends in a terminal frame is shortened to
 * {@link TERMINAL_OWNERSHIP_GRACE_MS}, so the long window only ever applies to
 * a session actually sitting parked.
 */
export const PARK_OWNERSHIP_GRACE_MS = 24 * 60 * 60 * 1000;

/**
 * 7 days — how long a claimed webhook idempotency key is kept.
 *
 * The bound only has to outlive every window in which the SAME delivery can
 * come back: Linear's own webhook redelivery window, a rolling deployment's
 * old/new revision overlap, and the 48h default `eventTtlMs` a claimed event
 * can sit queued for. A week clears all three with room to spare, and at
 * router-scale webhook volumes the table stays tiny.
 */
export const DEFAULT_WEBHOOK_CLAIM_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;
/** Completed run observations remain queryable for one day. */
export const DEFAULT_AGENT_RUN_RETENTION_MS = 24 * 60 * 60 * 1000;

/**
 * Routes Linear agent-session webhooks to the creator's enrolled device.
 *
 * Enforces session/issue affinity, per-issue locking, creator-only prompting,
 * and offline queueing with one-time notices. A periodic {@link sweepExpired}
 * fails out events that outlived their TTL and reclaims locks stranded by
 * devices that went dark.
 */
export class EventRouter {
	private readonly store: RouterStore;
	private readonly gateway: Pick<DeviceGateway, "isOnline" | "deliverPending">;
	private readonly postActivity: (
		workspaceId: string,
		agentSessionId: string,
		body: string,
	) => Promise<void>;
	private readonly moveIssueToStartedState:
		| ((workspaceId: string, issueId: string) => Promise<string | undefined>)
		| undefined;
	private readonly fetchRoutingContext:
		| EventRouterOptions["fetchRoutingContext"]
		| undefined;
	private readonly containerTargets: ContainerTargetService | undefined;
	private readonly devcontainers: DevcontainerImageService | undefined;
	private readonly repositoryRegistry: RepositoryRegistry | undefined;
	/** Issues we've already posted a "Building image…" notice for. */
	private readonly devcontainerNotified = new Set<string>();
	private readonly repositoryResolver: RepositoryResolver | undefined;
	private readonly postRepositorySelection:
		| EventRouterOptions["postRepositorySelection"]
		| undefined;
	private readonly terminalTeardown: TerminalTeardown | undefined;
	private readonly config: {
		eventTtlMs: number;
		issueLock: boolean;
		creatorOnlyPrompting: boolean;
		/** An undeclared affinity row younger than this is never reclaimed — it may
		 *  belong to a session the device was routed but has not started tracking. */
		affinityGraceMs: number;
	};
	private readonly webhookClaimRetentionMs: number;
	private readonly logger: ILogger;
	private readonly now: () => number;

	/** Sessions we've already posted an offline notice for (once-per-session). */
	private readonly notifiedSessions = new Set<string>();
	/**
	 * In-memory session -> workspace map so {@link sweepExpired}'s stale-lock
	 * pass can address the offline-release activity. The DB does not persist a
	 * workspace on locks; a router restart simply loses this hint (the lock is
	 * still released, only the courtesy post may be skipped).
	 */
	private readonly sessionWorkspace = new Map<string, string>();
	/**
	 * Sessions currently parked, mapped to the device that parked them and the
	 * `creator_json` their affinity row carried before the park deleted it
	 * (`undefined` when they had none).
	 *
	 * Two jobs. The entry authorizes an `active` frame — only a session this
	 * router saw park, *from the device that parked it*, may have its affinity
	 * restored. The value restores `creator_json` intact, which matters because
	 * that field gates who may prompt the session; writing it back as NULL would
	 * quietly discard the record.
	 *
	 * `deviceId` is load-bearing, not bookkeeping. `active` writes full session
	 * affinity — the strongest of the three ownership routes — and this map is
	 * the only thing it consults first. Keyed by session id alone, any enrolled
	 * device could unpark a session another device parked and take its affinity;
	 * the park branch's ownership gate stops a forged entry being created, and
	 * this field stops a genuine entry being redeemed by the wrong device.
	 *
	 * In-memory for the same reason as {@link sessionWorkspace}: a router
	 * restart loses the hint, and the creator check falls back to the webhook's
	 * `agentSession.creator` exactly as it already does after a terminal state.
	 */
	private readonly parkedSessionCreators = new Map<
		string,
		{ deviceId: number; creator: string | undefined }
	>();

	constructor(opts: EventRouterOptions) {
		this.store = opts.store;
		this.gateway = opts.gateway;
		this.postActivity = opts.postActivity;
		this.moveIssueToStartedState = opts.moveIssueToStartedState;
		this.fetchRoutingContext = opts.fetchRoutingContext;
		this.containerTargets = opts.containerTargets;
		this.repositoryResolver = opts.repositoryResolver;
		this.devcontainers = opts.devcontainers;
		this.repositoryRegistry = opts.registry;
		// A build finishing is the only thing that can release a webhook this
		// gate held, and it happens on a timer this class does not own — so the
		// release is a callback registered here rather than a poll.
		this.devcontainers?.setOnBuildFinished((cacheKey) => {
			void this.releaseHeldDevcontainerBuilds(cacheKey);
		});
		this.postRepositorySelection = opts.postRepositorySelection;
		this.terminalTeardown = opts.terminalTeardown;
		this.config = opts.config;
		this.webhookClaimRetentionMs =
			opts.config.webhookClaimRetentionMs ?? DEFAULT_WEBHOOK_CLAIM_RETENTION_MS;
		this.logger = opts.logger;
		this.now = opts.now ?? Date.now;
	}

	/**
	 * Route one inbound webhook.
	 *
	 * The span wrapping this is the pivot of the whole trace. `RouterServer`
	 * invokes it as `void route(event)` from inside the webhook handler, so it
	 * inherits the HTTP server span's context but outlives the HTTP response —
	 * which is the honest shape of the work and exactly what a trace should
	 * show. Everything downstream (target resolution, repository selection,
	 * container boot, the event the sandbox worker eventually picks up) hangs
	 * off this span.
	 */
	async route(event: AgentEvent): Promise<void> {
		const webhook = event as unknown as Webhook;
		return withSpan(
			routerTracer(),
			ROUTER_SPANS.route,
			{
				// CONSUMER, not SERVER: the HTTP exchange is already covered by the
				// Fastify span above this one. What this span measures is the
				// consumption of a message that arrived over it.
				kind: SpanKind.CONSUMER,
				attributes: cyrusSpanAttributes({
					webhook_type: webhook.type,
					webhook_action: webhook.action,
				}),
			},
			() => this.routeInner(webhook),
		);
	}

	private async routeInner(webhook: Webhook): Promise<void> {
		// Duplicate-delivery gate, BEFORE any routing work. A rolling deployment
		// overlaps the outgoing and incoming router revision, so the same webhook
		// (a Linear retry, or the same durable work replayed after a restart) can
		// reach the router twice; the 2026-07-26 emergency rollout did exactly
		// that and produced doubled Linear activity and two `linear-mcp-ok`
		// comments for one surviving worker. Claim first so no side effect —
		// queue row, issue lock, Linear activity, container boot, MCP mutation —
		// can happen twice for one delivery.
		if (!this.claimWebhook(webhook)) return;
		if (isAgentSessionPromptedWebhook(webhook)) {
			await this.routePrompted(webhook);
			return;
		}
		if (isAgentSessionCreatedWebhook(webhook)) {
			await this.routeCreated(webhook);
			return;
		}
		if (isIssueNewCommentWebhook(webhook)) {
			await this.routeIssueOwnerComment(webhook);
			return;
		}
		// Terminal-state webhooks carry no agent session, so they route on issue
		// affinity rather than through resolveTarget(). The device needs them to
		// run its own terminal-state cleanup (stop sessions, cyrus-teardown.sh,
		// remove worktrees) — without this forwarding a node's worktrees are
		// never reclaimed, since the node has no other way to learn an issue
		// closed. See EdgeWorker.handleIssueStateChangeMessage.
		if (isIssueStateChangeWebhook(webhook)) {
			await this.routeIssueTerminal(
				webhook,
				webhook.notification?.issue,
				"closed",
			);
			return;
		}
		if (isIssueDeletedWebhook(webhook)) {
			await this.routeIssueTerminal(webhook, webhook.data, "deleted");
			return;
		}
		this.logger.info(
			`EventRouter ignoring non-agent-session webhook ${webhook.type}/${webhook.action}`,
		);
	}

	/**
	 * Forward a regular issue comment to the device already owning that issue.
	 *
	 * The notification has no agent-session id, so the router cannot safely
	 * choose a session itself. EdgeWorker performs that final, creator-only and
	 * ambiguity-checked selection after delivery. An issue-affinity lookup keeps
	 * this constrained to the existing owner rather than treating every comment
	 * as a new routing request.
	 */
	private async routeIssueOwnerComment(webhook: Webhook): Promise<void> {
		const issue = (
			webhook as {
				notification?: { issue?: { id?: string; identifier?: string } };
			}
		).notification?.issue;
		const issueId = issue?.id;
		if (!issueId) return;

		const deviceId = this.store.getIssueAffinity(issueId);
		if (deviceId === undefined) return;

		this.store.enqueueEvent(
			deviceId,
			JSON.stringify(webhook),
			this.now(),
			this.config.eventTtlMs,
			injectTraceContext(),
		);
		if (this.gateway.isOnline(deviceId)) {
			this.gateway.deliverPending(deviceId);
		}
		this.logger.info(
			`Forwarded ordinary comment for issue ${issue?.identifier ?? issueId} to device ${deviceId}`,
		);
	}

	/**
	 * Claims this delivery's idempotency key in SQLite, returning whether the
	 * caller may go on to route it. The claim is transactional and arbitrated by
	 * a UNIQUE constraint (see {@link RouterStore.claimWebhookEvent}), so two
	 * router replicas sharing one database resolve to exactly one winner.
	 *
	 * Fails OPEN in two cases, both of which route the event unprotected rather
	 * than drop it:
	 * - The payload yields no key material at all (no `createdAt` — see
	 *   {@link webhookIdempotencyKey}). Collapsing two distinct events onto a
	 *   degenerate shared key would silently discard real work. No real Linear
	 *   webhook takes this path.
	 * - The store itself throws. A DB error is not evidence of a duplicate, and
	 *   letting it escape would reject out of `route()` — which RouterServer
	 *   invokes as `void route(event)` with no catch, so an unhandled rejection
	 *   would take the whole router process down for every teammate. If the
	 *   database really is unusable, `enqueueEvent` fails loudly a moment later
	 *   for this one event instead.
	 */
	private claimWebhook(webhook: Webhook): boolean {
		const label = `${webhook.type}/${webhook.action}`;
		const key = webhookIdempotencyKey(webhook);
		if (key === undefined) {
			this.logger.warn(
				`Webhook ${label} carries no idempotency key material; routing it without duplicate protection`,
			);
			return true;
		}
		let claimed: boolean;
		try {
			claimed = this.store.claimWebhookEvent(key, this.now());
		} catch (err) {
			this.logger.error(
				`Could not claim idempotency key ${key} for webhook ${label}; routing it without duplicate protection`,
				err,
			);
			return true;
		}
		if (claimed) return true;
		this.logger.info(
			`Dropping duplicate webhook ${label}: idempotency key ${key} was already claimed`,
		);
		return false;
	}

	/**
	 * Forwards a terminal-state webhook (issue completed/canceled, or issue
	 * deleted) to the device that owns the issue, so it can reclaim the
	 * worktree.
	 *
	 * Routes on `issue_affinity` — the only mapping that survives the session
	 * ending. Session affinity and the issue lock are both torn down the moment
	 * a session reports a terminal `session_state`, which for a typical issue
	 * happens well BEFORE the human moves it to Done. Issue affinity is only
	 * purged when the device itself is removed, so it still points at the right
	 * machine days later — which is exactly the window this cleanup lives in.
	 *
	 * No Linear activity is posted on the failure paths: a status change is not
	 * an agent session, so there is no thread to post to.
	 */
	private async routeIssueTerminal(
		webhook: Webhook,
		issue: { id?: string; identifier?: string } | null | undefined,
		action: TerminalTeardownAction,
	): Promise<void> {
		const label = `${webhook.type}/${webhook.action}`;
		const issueId = issue?.id;
		if (!issueId) {
			this.logger.warn(
				`Terminal webhook ${label} carries no issue id; cannot route cleanup`,
			);
			return;
		}
		const issueRef = issue?.identifier ?? issueId;

		const deviceId = this.store.getIssueAffinity(issueId);
		if (deviceId === undefined) {
			if (action === "deleted" && this.terminalTeardown && issue?.identifier) {
				try {
					await this.terminalTeardown.deleteRetainedBundle(issue.identifier);
					this.logger.info(
						`Removed retained artifact bundle for deleted issue ${issueRef}`,
					);
				} catch (error) {
					this.logger.warn(
						`Could not remove retained artifact bundle for deleted issue ${issueRef}`,
						error,
					);
				}
			}
			// No device ever ran a session for this issue, so no device holds a
			// worktree for it. Nothing to clean up — not an error.
			this.logger.info(
				`Terminal webhook ${label} for issue ${issueRef}: no device affinity, nothing to clean up`,
			);
			return;
		}

		// Enqueue unconditionally rather than only when online: the worktree
		// still needs reclaiming when the device comes back, and pendingEvents
		// replays anything unacked on reconnect. Cleanup is idempotent on the
		// node (deleteWorktree no-ops when the directory is already gone), so a
		// duplicate delivery is harmless.
		this.store.enqueueEvent(
			deviceId,
			JSON.stringify(webhook),
			this.now(),
			this.config.eventTtlMs,
			injectTraceContext(),
		);
		if (this.gateway.isOnline(deviceId)) {
			this.gateway.deliverPending(deviceId);
		}
		this.logger.info(
			`Forwarded terminal webhook ${label} for issue ${issueRef} to device ${deviceId} for worktree cleanup`,
		);

		if (
			this.containerTargets?.isContainerDevice(deviceId) &&
			this.terminalTeardown
		) {
			const issueKey = this.store.getDeviceInfo(deviceId)?.issueKey;
			if (!issueKey) {
				this.logger.warn(
					`Container device ${deviceId} has no issue key; cannot register terminal teardown`,
				);
				return;
			}
			if (this.terminalTeardown.register({ issueKey, deviceId, action })) {
				this.containerTargets.bootForTeardown(deviceId);
			}
		}
	}

	/**
	 * Applies a `session_state` frame.
	 *
	 * Terminal states (complete / error / stopped) release the issue lock AND
	 * session affinity, and forget the session's in-memory bookkeeping.
	 *
	 * `parked` is NOT terminal: the session is blocked on a user answer with no
	 * work in flight. It releases session affinity ONLY — which is what lets
	 * {@link ContainerLifecycle} idle-stop the container — while keeping:
	 *  - the issue lock, so no other session claims the issue mid-conversation;
	 *  - `notifiedSessions`/`sessionWorkspace`, which the session still needs
	 *    when the user's answer resumes it.
	 * It also stamps the park time, which the sweep uses as its idle clock.
	 *
	 * `active` reverses a park the user never answered — the elicitation was
	 * abandoned, replaced, or failed to post, and the agent went back to work.
	 * It restores affinity to the sending device and clears the idle stamp,
	 * keeping the issue lock exactly as `parked` did. Without it a park is
	 * one-way: the session keeps running with no affinity, so every
	 * session-scoped RPC it makes is rejected with "session not owned by this
	 * device" and the entire turn is lost in silence (PAR-146).
	 *
	 * All paths are idempotent: the device replays unacked frames on
	 * reconnect, and the router acks only after applying.
	 */
	/**
	 * Emit a sandbox lifecycle event for a device, if it is a container.
	 *
	 * `handleSessionState` is shared by physical devices and sandboxes, but the
	 * `sandbox_*` vocabulary is only meaningful for the latter — a teammate's
	 * laptop parking a session is not a fleet-cost signal. Silently skipping
	 * non-container devices keeps the event stream something an operator can
	 * count sandboxes from.
	 *
	 * Never throws: telemetry must not be able to fail a state transition the
	 * store has already applied.
	 */
	private emitSandboxLifecycle(
		deviceId: number,
		name: SandboxEventName,
		attributes?: LogEventAttributes,
	): void {
		try {
			const device = this.store.getDeviceInfo(deviceId);
			if (
				device?.kind !== "container" ||
				!device.issueKey ||
				!device.provider
			) {
				return;
			}
			emitSandboxEvent(
				this.logger,
				name,
				{ issueKey: device.issueKey, deviceId, provider: device.provider },
				attributes,
			);
		} catch (err) {
			this.logger.warn(
				`Failed to emit ${name} telemetry for device ${deviceId}`,
				err,
			);
		}
	}

	/**
	 * Refuse a device's claim on a session: one queryable event plus one WARN.
	 *
	 * Both halves are needed and neither substitutes for the other — `event()`
	 * carries the closed-set `cyrus.reason` a KQL `summarize` groups on and
	 * bypasses the sink's level threshold, WARN is what an operator reading a
	 * console actually sees. NOR-405 was invisible for a day for want of exactly
	 * this pair on the RPC path.
	 */
	private refuseSessionOwnership(
		refusal: SessionOwnershipRefusal,
		message: string,
	): void {
		// The run the refused claim was aimed at, so a refusal is filterable by the
		// same workspace/owner/team/project columns as the run itself. Read once
		// and shared by both halves — the event and the WARN — because an operator
		// pivoting from one to the other must not find them disagreeing.
		const run = refusal.sessionId
			? this.store.getAgentRunForSession(refusal.sessionId)
			: undefined;
		const attribution = run ? runAttribution(run) : undefined;
		emitSessionOwnershipRefusal(this.logger, {
			...refusal,
			...(attribution ? { attribution } : {}),
		});
		this.logger
			.withContext({
				...(refusal.sessionId !== undefined
					? { sessionId: refusal.sessionId }
					: {}),
				attributes: cyrusAttributes({
					reason: refusal.reason,
					device_id: refusal.deviceId,
					session_id: refusal.sessionId ?? null,
					session_state: refusal.sessionState ?? null,
					owner_device_id: refusal.ownerDeviceId ?? null,
				}),
			})
			.warn(message);
	}

	handleSessionState(deviceId: number, frame: SessionStateFrame): void {
		// Read BEFORE any branch mutates the rows it is derived from. `deviceId`
		// is the authenticated socket's device, but `frame.sessionId` is
		// device-supplied and unvalidated beyond being a non-empty string — so
		// every grant below must be gated on this, never on the sender alone.
		// Granting on the sender would turn a stray or forged terminal frame into
		// a way to MINT authorization over someone else's session.
		//
		// "Grant" means every claim a branch can create, not just the grace rows:
		// the waiting branch's in-memory entry authorizes a later `active`, and
		// `active` writes affinity outright. All three are gated. What is NOT
		// gated is the terminal branch's RELEASES — see there for why.
		const owner = this.store.getSessionOwner(frame.sessionId, this.now());
		if (frame.state === "parked" || frame.state === "waiting") {
			const now = this.now();
			// The legacy `parked` frame conflated two facts. Reading it as
			// waiting-on-elicitation with executor parking is not an inference about
			// a silent worker — it is what a worker sending that frame has always
			// explicitly meant, and it is the only thing that frame was ever sent
			// for. The `since` it never carried falls back to now, which is the
			// instant the router learned of the wait.
			const legacy = frame.state === "parked";
			const wait: AgentRunWait = legacy
				? { reason: "elicitation", sinceMs: now }
				: narrowWaitReason(frame.wait, now);
			// The run being blocked and its container being suspendable are
			// independent, and only the second one releases affinity. A worker with
			// a live background build reports `waiting` with no park permission:
			// suspending it would freeze the build, and the completion that would
			// wake the session could then never arrive. Absent reads as "do not
			// park" — the direction that costs a suspend rather than a stall.
			const mayPark = legacy ? true : frame.executorMayPark === true;
			// Gated as a WHOLE, not just the grace grant below. `parkedSessionCreators`
			// is the only thing the `active` branch consults before writing full
			// session affinity, so an ungated `set` here is itself an escalation
			// primitive: a forged park that merely seeds the map lets a following
			// forged `active` mint affinity over someone else's session — a strictly
			// stronger claim than the grace this gate refuses. Two frames, not one.
			//
			// Refusing outright is safe here in a way it is NOT on the terminal path.
			// A park deliberately RETAINS the issue lock, so declining to act strands
			// nothing: the rightful owner keeps every claim it already had, and the
			// issue stays delegatable. The terminal branch must still run its
			// releases for exactly the opposite reason.
			//
			// Applies to a non-parking wait too, even though that one grants nothing
			// durable: `setAgentRunState` writes to the run row of whatever session
			// the frame names, so an ungated path would let any enrolled device
			// label someone else's run as waiting.
			if (owner !== deviceId) {
				this.refuseSessionOwnership(
					{
						reason: "park_not_owned",
						sessionId: frame.sessionId,
						deviceId,
						ownerDeviceId: owner,
						sessionState: frame.state,
					},
					`Device ${deviceId} reported '${frame.state}' for session ${frame.sessionId}, which it does not own (owner: ${owner ?? "none"}); ignored`,
				);
				return;
			}

			// The RUN is waiting either way — that is the worker's report, and it is
			// recorded before any executor decision, so a run blocked with a live
			// background build is observable as waiting rather than as silence.
			this.store.setAgentRunState(frame.sessionId, "waiting", {
				wait,
				...readFrameRunFacts(frame),
			});

			if (!mayPark) {
				// Nothing durable is released: affinity stays, the idle stamp is not
				// set, and no in-memory park is recorded — so no later `active` can
				// redeem one. The session simply keeps running, blocked, on a
				// container that must stay up.
				this.logger.info(
					`Session ${frame.sessionId} is waiting on device ${deviceId} (${wait.reason}) but its executor must not park; kept affinity`,
				);
				return;
			}

			// Stashed, not dropped: `clearSessionAffinity` deletes the row that
			// carries `creator_json`, and that field is the gate on who may prompt
			// this session. Holding it here lets `active` put it back intact.
			// In-memory by the same reasoning as `notifiedSessions` below — a
			// router restart re-derives it from the webhook's `agentSession.creator`.
			// Always recorded, even when the creator is absent: the entry is what
			// tells `active` this device really parked the session.
			this.parkedSessionCreators.set(frame.sessionId, {
				deviceId,
				creator: this.store.getSessionCreator(frame.sessionId),
			});
			// The issue lock covers this window and outlasts any park — but ONLY on
			// a deployment with `issueLock` enabled. With it off no lock row is ever
			// taken, so without this grant the park drops the device's last claim
			// and every activity a parked session posts is rejected: precisely the
			// 40-post PAR-275 burst, unfixed (NOR-405).
			this.store.grantSessionOwnershipGrace(
				frame.sessionId,
				deviceId,
				now + PARK_OWNERSHIP_GRACE_MS,
			);
			this.store.clearSessionAffinity(frame.sessionId);
			this.store.setDeviceParkedAt(deviceId, now);
			this.logger.info(
				`Session ${frame.sessionId} is waiting on device ${deviceId} (${wait.reason}); parked its executor, released affinity, retained the issue lock`,
			);
			this.emitSandboxLifecycle(deviceId, SANDBOX_EVENTS.parked, {
				session_id: frame.sessionId,
			});
			return;
		}
		if (frame.state === "active") {
			// Only for a session this device actually parked. A replayed or stray
			// `active` must not mint ownership out of nothing.
			const parked = this.parkedSessionCreators.get(frame.sessionId);
			if (parked === undefined) {
				// No park to redeem — but the frame is still the worker explicitly
				// saying this run is progressing, and it carries the facts (runner,
				// model, pending work) that keep a long-running session observable.
				// Recording them is what stops a run held open by a seven-hour cron
				// from looking like silence.
				//
				// Gated on ownership, and grants NOTHING: no affinity, no grace, no
				// park entry. The pre-run-facts behaviour for a stray `active` — log
				// and ignore — is preserved exactly for a device that does not own
				// the session.
				if (owner === deviceId) {
					this.store.setAgentRunState(
						frame.sessionId,
						"active",
						readFrameRunFacts(frame),
					);
					return;
				}
				this.logger.info(
					`Ignoring 'active' for session ${frame.sessionId} on device ${deviceId}: no park on record`,
				);
				return;
			}
			// The park was genuine, but this is not the device that made it. Below
			// this line is `setSessionAffinity` — the strongest ownership route
			// there is — so matching on the session id alone would let any enrolled
			// device redeem another device's park and take its session.
			//
			// The entry is deliberately NOT deleted on this path: dropping it would
			// turn a forged `active` into a denial of service against the rightful
			// owner's own unpark, trading an escalation for a hang.
			if (parked.deviceId !== deviceId) {
				this.refuseSessionOwnership(
					{
						reason: "unpark_not_parker",
						sessionId: frame.sessionId,
						deviceId,
						ownerDeviceId: parked.deviceId,
						sessionState: frame.state,
					},
					`Device ${deviceId} reported 'active' for session ${frame.sessionId}, which device ${parked.deviceId} parked; ignored`,
				);
				return;
			}
			const creator = parked.creator;
			this.parkedSessionCreators.delete(frame.sessionId);
			// setSessionAffinity clears the device's park stamp as a side effect.
			this.store.setSessionAffinity(frame.sessionId, deviceId, creator);
			// Affinity is back, so the park grace has nothing left to cover. Drop it
			// rather than let it idle out: ownership should rest on the narrowest
			// claim that supports it, and a terminal frame later re-grants the much
			// shorter terminal window from scratch.
			this.store.clearSessionOwnershipGrace(frame.sessionId);
			this.store.setAgentRunState(
				frame.sessionId,
				"active",
				readFrameRunFacts(frame),
			);
			this.logger.info(
				`Session ${frame.sessionId} unparked on device ${deviceId}; restored affinity and cleared the idle stamp`,
			);
			this.emitSandboxLifecycle(deviceId, SANDBOX_EVENTS.unparked, {
				session_id: frame.sessionId,
			});
			return;
		}
		// Same reasoning as the `active` branch: only the device that parked a
		// session may retire its entry. The releases below are deliberately
		// ungated, but this map is an authorization record, and clearing another
		// device's park would strand its unpark behind "no park on record".
		const parkedBySender =
			this.parkedSessionCreators.get(frame.sessionId)?.deviceId === deviceId;
		if (parkedBySender) {
			this.parkedSessionCreators.delete(frame.sessionId);
		}
		const now = this.now();
		// Read before `finishAgentRun` moves the row to a terminal state, so a
		// replay can still be told apart from a stray frame afterwards.
		const priorRun = this.store.getAgentRunForSession(frame.sessionId);
		// The terminal frame is the ONLY point at which an ordinary run — one that
		// never waited and never deferred for pending work — offers its runner and
		// model. Dropping them here left that run with `runner = NULL` for good.
		const ended = this.store.finishAgentRun(
			frame.sessionId,
			frame.state,
			now,
			readFrameRunFacts(frame),
		);
		// After the write, so the runner and model the terminal frame just supplied
		// are on the line. For an ordinary run — one that never waited and never
		// deferred — this frame is the ONLY point those two facts are ever
		// reported, so emitting before the write would leave every such run's
		// closing record saying `runner = null`.
		//
		// Gated on `ended`, so the replayed terminal frame handled a few lines
		// below emits nothing rather than a second `run.finished` for the same run.
		this.emitRunLifecycle(
			frame.sessionId,
			RUN_EVENTS.finished,
			{ terminal_state: frame.state, reporting_device_id: deviceId },
			ended,
		);
		// Granted BEFORE the two releases below, so there is no instant in which
		// the terminating device owns the session by none of the routes
		// `getSessionOwner` checks. Unlike the park path, which keeps the issue
		// lock, this frame drops every durable claim the device has — and the
		// activities still in flight when it lands are the session's closing
		// summary, which is the part of a run a user most wants to read (NOR-405).
		//
		// Gated on prior ownership, and note this SHORTENS a park grace rather
		// than extending it: a parked session that then completes gets the
		// terminal window, not the much longer park one.
		if (owner === deviceId) {
			this.store.grantSessionOwnershipGrace(
				frame.sessionId,
				deviceId,
				now + TERMINAL_OWNERSHIP_GRACE_MS,
			);
		} else if (priorRun?.deviceId === deviceId) {
			// A replay, not an intrusion. `RouterServer` applies a `session_state`
			// frame BEFORE acking it, so a process death in between leaves the device
			// replaying a frame the router already applied; that design is documented
			// as safe because `handleSessionState` is idempotent. Once the grace has
			// lapsed the replay arrives owning nothing, and warning about it would
			// seed the one detector for genuine frame forgery with routine false
			// positives — the same signal-quality failure the sandbox sweep's
			// `idle_stop_skipped` reasons exist to avoid.
			//
			// Grants nothing: the window is measured from the terminal instant, not
			// from whenever the ack happened to land, so a replay must not re-open it.
			this.logger.info(
				`Ignoring replayed terminal state '${frame.state}' for session ${frame.sessionId} from device ${deviceId}: run already ended, grace lapsed`,
			);
		} else {
			// Not fatal — the releases below still run, because refusing them would
			// strand the issue lock and leave the issue permanently undelegatable,
			// which is worse than the stray frame. But a device reporting a
			// terminal state for a session it does not own is either a bug or an
			// attempt to act on someone else's session, and neither should be
			// silent.
			//
			// The grace row is deliberately NOT cleared. It belongs to whichever
			// device does own the session, it expires on its own, and deleting it
			// here would let any enrolled device cancel another's posting window
			// with a single frame — destroying the protection this PR added, one
			// line after refusing to grant that same device anything.
			this.refuseSessionOwnership(
				{
					reason: "terminal_not_owned",
					sessionId: frame.sessionId,
					deviceId,
					ownerDeviceId: owner,
					sessionState: frame.state,
				},
				`Device ${deviceId} reported terminal state '${frame.state}' for session ${frame.sessionId}, which it does not own (owner: ${owner ?? "none"}); released the session's rows but granted no posting grace`,
			);
		}
		this.store.releaseIssueLockForSession(frame.sessionId);
		this.store.clearSessionAffinity(frame.sessionId);
		this.notifiedSessions.delete(frame.sessionId);
		this.sessionWorkspace.delete(frame.sessionId);
		this.logger.info(
			`Session ${frame.sessionId} reached terminal state '${frame.state}' on device ${deviceId}; released lock and affinity (${TERMINAL_OWNERSHIP_GRACE_MS}ms posting grace)`,
		);
	}

	/**
	 * Reclaims issue locks a reconnecting device no longer backs with a live
	 * session. A device declares its currently-tracked session IDs in the hello
	 * frame; any lock we hold for that device whose session is not in the
	 * declared set belongs to a session the device has lost — classically to a
	 * corrupted state file after an ENOSPC restart — and can therefore never be
	 * released by a terminal frame. Without this the issue stays locked forever
	 * and every re-delegation is rejected.
	 *
	 * Fires on "deviceConnected". Safe to call for every reconnect; it only acts
	 * on locks the device didn't claim.
	 *
	 * Guards:
	 * - `declaredSessions === undefined` → an older client that doesn't report
	 *   active sessions. Reclaiming would wrongly release every lock, so skip
	 *   entirely and keep pre-reconcile behavior.
	 * - The device still has undelivered events → it isn't caught up. A queued
	 *   `created` event will make it start tracking a session it can't declare
	 *   yet, so its list isn't authoritative; defer to a later reconnect. (Acked
	 *   events are deleted, so this only trips while delivery is genuinely
	 *   behind.)
	 */
	async reconcileDeviceLocks(
		deviceId: number,
		declaredSessions: string[] | undefined,
	): Promise<void> {
		if (!this.config.issueLock) return;
		if (declaredSessions === undefined) return;

		if (this.store.hasPendingEvents(deviceId, this.now())) {
			this.logger.info(
				`Skipping lock reconciliation for device ${deviceId}: it has undelivered events, so its active-session list is not yet authoritative`,
			);
			return;
		}

		const declared = new Set(declaredSessions);
		const locks = this.store.getIssueLocksForDevice(deviceId);

		// Two passes on purpose. Do every DB release synchronously first, before
		// any `await`, so a `session_state` frame the device replays right after
		// this same hello can't interleave mid-loop and race a release. Only then
		// do the (awaiting) courtesy posts.
		const reclaimed: Array<{
			issueId: string;
			sessionId: string;
			workspaceId: string | undefined;
		}> = [];
		for (const { issueId, sessionId } of locks) {
			if (declared.has(sessionId)) continue;
			const ended = this.store.markAgentRunUnknown(sessionId, this.now());
			this.emitRunLifecycle(
				sessionId,
				RUN_EVENTS.unknown,
				{
					reason: "lock_reconciled" satisfies RunUnknownReason,
					reporting_device_id: deviceId,
				},
				ended,
			);
			this.store.releaseIssueLockForSession(sessionId);
			this.store.clearSessionAffinity(sessionId);
			const workspaceId = this.sessionWorkspace.get(sessionId);
			this.notifiedSessions.delete(sessionId);
			this.sessionWorkspace.delete(sessionId);
			reclaimed.push({ issueId, sessionId, workspaceId });
		}

		for (const { issueId, sessionId, workspaceId } of reclaimed) {
			// Best-effort courtesy post. The workspace hint is in-memory only, so
			// after a router restart we usually can't address the Linear thread —
			// the lock release (the part that unblocks re-delegation) still stands.
			if (workspaceId) {
				await this.postActivity(
					workspaceId,
					sessionId,
					ORPHANED_LOCK_RECLAIMED_MESSAGE,
				);
			}
			this.logger.info(
				`Reclaimed orphaned lock for issue ${issueId}: device ${deviceId} reconnected without tracking session ${sessionId}`,
			);
		}
	}

	/**
	 * Re-derives a device's affinity set from what the device says it is running,
	 * and returns the count that remains.
	 *
	 * Affinity is written on routing and cleared only by a terminal frame the
	 * worker may never send — `routePrompted` re-establishes it for an
	 * already-terminal session (deliberately; a Linear agent session outlives its
	 * turns), takes no issue lock, and logs nothing when the device is online. If
	 * that session never goes terminal again the row is permanent, and
	 * `ContainerLifecycle.sweep` skips the device at its affinity gate forever.
	 * That is PAR-146: parked correctly, then ran 28+ minutes at 4 vCPU / 8 GiB.
	 *
	 * Two guards, both required:
	 * - `declared === undefined` means the device could not tell us. Reclaiming
	 *   would be a guess, so we do nothing.
	 * - A row younger than `affinityGraceMs` is kept even when undeclared: it may
	 *   have been routed seconds ago with the event still queued, so the device
	 *   genuinely cannot declare it yet.
	 */
	/**
	 * Drops the IN-MEMORY ownership records this router holds for a session whose
	 * durable ownership has just been released by something outside this class —
	 * today, guarded recovery.
	 *
	 * `parkedSessionCreators` is the reason this exists rather than being folded
	 * into the store write. It is the only thing the `active` branch of
	 * {@link handleSessionState} consults before calling `setSessionAffinity`, and
	 * that call grants the strongest ownership there is with no further check once
	 * the entry names the reporting device. So a stray or late `active` frame from
	 * a device whose ownership was just released would re-pin a run that is now
	 * `unknown` — and a run that has ended can never end again, which is PAR-146's
	 * permanent pin arriving by a new route.
	 *
	 * Scoped by device, matching every durable release: another device's park is
	 * not this caller's to forget. `notifiedSessions` / `sessionWorkspace` go with
	 * it, exactly as `reconcileDeviceLocks` clears them on its own reclaim path.
	 *
	 * @returns whether a park record was actually retired.
	 */
	forgetSessionOwnership(sessionId: string, deviceId: number): boolean {
		const parked = this.parkedSessionCreators.get(sessionId);
		const retired = parked?.deviceId === deviceId;
		if (retired) this.parkedSessionCreators.delete(sessionId);
		this.notifiedSessions.delete(sessionId);
		this.sessionWorkspace.delete(sessionId);
		return retired;
	}

	reconcileDeviceAffinity(
		deviceId: number,
		declared: string[] | undefined,
		nowMs: number,
	): number {
		const rows = this.store.listSessionAffinityForDevice(deviceId);
		if (declared === undefined) return rows.length;

		const declaredSet = new Set(declared);
		let remaining = 0;
		for (const { sessionId, establishedMs } of rows) {
			if (declaredSet.has(sessionId)) {
				remaining++;
				continue;
			}
			if (nowMs - establishedMs < this.config.affinityGraceMs) {
				remaining++;
				continue;
			}
			const ended = this.store.markAgentRunUnknown(sessionId, nowMs);
			this.emitRunLifecycle(
				sessionId,
				RUN_EVENTS.unknown,
				{
					reason: "affinity_reconciled" satisfies RunUnknownReason,
					reporting_device_id: deviceId,
				},
				ended,
			);
			this.store.clearSessionAffinity(sessionId);
			this.logger.info(
				`Reclaimed stale affinity for session ${sessionId} on device ${deviceId}: ` +
					`the device does not report it running (established ${nowMs - establishedMs}ms ago, ` +
					`grace ${this.config.affinityGraceMs}ms)`,
			);
		}
		return remaining;
	}

	async sweepExpired(): Promise<void> {
		const now = this.now();

		// 1. Fail out events that outlived their TTL before delivery.
		for (const row of this.store.expireEvents(now)) {
			const session = this.asSessionEvent(row.payloadJson);
			if (!session) {
				this.logger.warn(
					`Dropping unparseable/unknown expired event on device ${row.deviceId}`,
				);
				continue;
			}
			const sessionId = session.agentSession.id;
			const workspaceId = session.organizationId;
			const email = session.agentSession.creator?.email ?? DEFAULT_EMAIL;
			await this.postActivity(workspaceId, sessionId, expiredMessage(email));
			this.logger.info(
				`Event for session ${sessionId} expired before delivery`,
			);

			// An undelivered created event never started work — free its issue so
			// it isn't held by a session that will never run.
			if (isAgentSessionCreatedWebhook(session)) {
				const ended = this.store.markAgentRunUnknown(sessionId, now);
				this.emitRunLifecycle(
					sessionId,
					RUN_EVENTS.unknown,
					{
						reason: "event_expired" satisfies RunUnknownReason,
						reporting_device_id: row.deviceId,
					},
					ended,
				);
				this.store.releaseIssueLockForSession(sessionId);
				this.store.clearSessionAffinity(sessionId);
			}
			this.notifiedSessions.delete(sessionId);
			this.sessionWorkspace.delete(sessionId);
		}

		// 2. Reclaim locks stranded by devices that went dark past the TTL, even
		//    for sessions whose event WAS delivered (Codex finding 10).
		const cutoff = now - this.config.eventTtlMs;
		for (const device of this.store.devicesOfflineSince(cutoff)) {
			const released = this.store.releaseLocksAndAffinityForDevice(
				device.deviceId,
				now,
			);
			for (const { sessionId } of released) {
				const workspaceId = this.sessionWorkspace.get(sessionId) ?? "";
				this.notifiedSessions.delete(sessionId);
				this.sessionWorkspace.delete(sessionId);
				await this.postActivity(
					workspaceId,
					sessionId,
					offlineReleaseMessage(device.email),
				);
				this.logger.info(
					`Released stale lock for session ${sessionId}; device ${device.deviceId} offline past TTL`,
				);
			}
		}

		// 3. Bounded retention for webhook idempotency claims. Nothing else ever
		//    deletes from that table, so this pass is what keeps it from growing
		//    for the life of the deployment.
		const claimsSwept = this.store.sweepWebhookClaims(
			now - this.webhookClaimRetentionMs,
		);
		if (claimsSwept > 0) {
			this.logger.info(
				`Swept ${claimsSwept} webhook idempotency claim(s) older than ${this.webhookClaimRetentionMs}ms`,
			);
		}
		this.store.sweepTerminalAgentRuns(now - DEFAULT_AGENT_RUN_RETENTION_MS);
		// Same reasoning as the claims above: `getSessionOwnershipGrace` drops a
		// lapsed row when it reads one, but the common case is a session that
		// completes and is never asked about again, so nothing would ever collect
		// it. Cutoff is `now`, not an age: the row already carries its own expiry.
		this.store.sweepSessionOwnershipGrace(now);

		// 4. A selection nobody ever answered. `eventTtlMs` is the right bound: it
		// is already how long a queued event may wait for its device. Pass 1
		// above already tells the user when their queued event expires
		// (`expiredMessage`); mirror that here so an abandoned repository
		// question doesn't just vanish silently — a user answering days later
		// would otherwise have their reply delivered as an ordinary prompt to a
		// session that was never created. The row's own workspace/session ids
		// are used rather than `sessionWorkspace` (that map is only populated
		// once a session reaches affinity, which a held session never has).
		for (const row of this.store.sweepPendingRepoSelections(
			now - this.config.eventTtlMs,
		)) {
			await this.postActivity(
				row.workspaceId,
				row.agentSessionId,
				REPOSITORY_SELECTION_EXPIRED_MESSAGE,
			);
			this.logger.info(
				`Swept an unanswered repository selection for issue ${row.issueKey} (session ${row.agentSessionId}); posted an expiry notice`,
			);
		}
	}

	/**
	 * Is this creator a KNOWN enrolled physical-device user — as opposed to a
	 * container-executor user, or a creator the router can't identify at all?
	 *
	 * Mirrors the exact check `resolveTarget`'s creator branch uses to make the
	 * SAME distinction (`findUserForCreator` -> `executorFor`), just earlier —
	 * before any container device is created — so router-side repository
	 * pre-selection (`ensureRepositoryDecision`) can be skipped for physical-
	 * device users while leaving it untouched for everyone else:
	 *
	 *   - container-executor user (`executorFor` returns a provider): `false`
	 *     — run the resolver, exactly as before.
	 *   - unrecognized creator (`findUserForCreator` finds no user, or there is
	 *     no `containerTargets` at all): `false` — also run the resolver. This
	 *     preserves the pre-existing, deliberate behaviour of asking an
	 *     unenrolled creator before `resolveTargetOrInvalidKey` would otherwise
	 *     tell them they're unenrolled (see `routeCreated`'s comment); it is
	 *     harmless because nothing on their device will ever answer a second
	 *     time, unlike a real enrolled physical device.
	 *   - enrolled, but `executorFor` returns `undefined` (the historical
	 *     meaning of "physical device", see `ContainerRoutingDeps`'s own doc
	 *     comment): `true` — skip the resolver. This is the one case a mixed
	 *     deployment (some container users, some physical-device users, both
	 *     behind the same `containers`-configured router) needs distinguished:
	 *     that user's own EdgeWorker already runs its own repository router,
	 *     so asking here too would double-elicit them for the same issue.
	 *
	 * KNOWN GAP (Minor review finding M-1, deliberately left open): this only
	 * mirrors `resolveTarget`'s CREATOR branch, not its three other
	 * fallbacks — session affinity, issue affinity, and parent-issue
	 * affinity — which `resolveTarget` itself checks in strict precedence
	 * order (session affinity -> creator -> issue affinity -> parent
	 * affinity). An unenrolled creator, or an app/automation-attributed
	 * creator `findUserForCreator` cannot resolve to a user, makes this
	 * return `false` even when the SAME issue already has affinity pointing
	 * at a physical device from an earlier, properly-attributed webhook —
	 * `routeCreated` then runs `ensureRepositoryDecision` for a webhook
	 * `resolveTarget` is about to route to that device via its affinity
	 * fallback, double-eliciting: once from the router, once from the
	 * device's own `RepositoryRouter`.
	 *
	 * A same-issue-affinity-implies-physical-device shortcut was tried and
	 * reverted: unlike this method, `resolveTarget` does NOT treat its four
	 * branches as unordered alternatives, so a plain OR of "is the creator a
	 * physical device" with "does the issue have affinity to a physical
	 * device" disagrees with `resolveTarget` whenever a LATER webhook on the
	 * SAME issue comes from a CONTAINER-executor creator: `resolveTarget`
	 * still routes it to that creator's container (creator branch precedes
	 * issue affinity), but the OR'd predicate returns `true` off the issue-
	 * affinity branch and skips the gate anyway — for a webhook that WILL
	 * boot a container. That is worse than the double-elicit this method
	 * exists to prevent: no decision is persisted, and `ContainerTargets`'
	 * repository fallback (`isDefault ?? the first repository in the
	 * workspace`) silently picks a repository nobody chose, with no
	 * elicitation even on a genuine tie. Closing this gap correctly requires
	 * mirroring `resolveTarget`'s full ORDERED precedence, not OR-ing its
	 * branches; that is deferred to a follow-up rather than attempted here.
	 */
	private isKnownPhysicalDeviceCreator(
		creator: SessionEvent["agentSession"]["creator"] | undefined,
	): boolean {
		if (!creator || !this.containerTargets) return false;
		const user = this.store.findUserForCreator({
			id: creator.id,
			email: creator.email,
		});
		if (!user) return false;
		return this.containerTargets.executorFor(user.userId) === undefined;
	}

	/**
	 * Ensures the issue has a repository decision before anything boots.
	 *
	 * Returns `"ready"` when routing may continue, and `"held"` when an
	 * elicitation was posted (or a blocking notice was) and this webhook must
	 * NOT be delivered. A held event is stashed verbatim and replayed by
	 * {@link resumeHeldSelection} once the user answers.
	 *
	 * Deliberately runs BEFORE `resolveTarget`: creating the container device
	 * first would mint a device row — and boot a sandbox — for an issue whose
	 * repository nobody has chosen yet. Waiting here costs nothing, which is the
	 * whole point of asking on the router rather than inside a container.
	 */
	private ensureRepositoryDecision(
		webhook: SessionEvent,
		issueKey: string,
		workspaceId: string,
		issueId: string | undefined,
	): Promise<"ready" | "held"> {
		// Worth its own span because `"held"` is a wait on a HUMAN — the router
		// posted an elicitation and nothing boots until someone answers. Without
		// this the gap shows up as an unexplained multi-minute hole between the
		// webhook and the boot.
		return withSpan(
			routerTracer(),
			ROUTER_SPANS.resolveRepository,
			{
				attributes: cyrusSpanAttributes({
					issue_key: issueKey,
					workspace_id: workspaceId,
				}),
			},
			async (span) => {
				const outcome = await this.ensureRepositoryDecisionInner(
					webhook,
					issueKey,
					workspaceId,
					issueId,
				);
				span.setAttributes(cyrusSpanAttributes({ outcome }));
				return outcome;
			},
		);
	}

	private async ensureRepositoryDecisionInner(
		webhook: SessionEvent,
		issueKey: string,
		workspaceId: string,
		issueId: string | undefined,
	): Promise<"ready" | "held"> {
		const resolver = this.repositoryResolver;
		if (!resolver) return "ready";
		if (this.store.getIssueRepositories(issueKey)) return "ready";

		const sessionId = webhook.agentSession.id;
		if (this.store.getPendingRepoSelection(sessionId)) {
			// A repeated `created` delivery for a session already waiting on an
			// answer. Asking again would post a second elicitation and overwrite
			// the held event with an identical one — silent, but noisy in Linear.
			this.logger.info(
				`Session ${sessionId} is already waiting on a repository selection; not asking again`,
			);
			return "held";
		}

		const outcome = await resolver.resolve({ workspaceId, issueId });

		if (outcome.kind === "resolved") {
			this.persistDecision(issueKey, outcome.decision);
			return "ready";
		}

		if (outcome.kind === "unavailable") {
			await this.postActivity(
				workspaceId,
				sessionId,
				fillTemplate(NO_REPOSITORIES_MESSAGE, { reason: outcome.reason }),
			);
			emitRoutingRejection(this.logger, {
				attribution: this.rejectionAttribution(webhook, sessionId),
				reason: "repositories_unavailable",
				sessionId,
				...(issueId !== undefined ? { issueId } : {}),
				issueKey,
			});
			this.logger.warn(`Cannot route session ${sessionId}: ${outcome.reason}`);
			// Stash the ORIGINAL `created` webhook rather than dropping it.
			// "unavailable" covers two causes — a transient registry read
			// failure (a first-boot Table race being the concrete case; the
			// fire-and-forget registry seed from RouterServer's constructor can
			// still be in flight when the very first webhook lands) and a
			// workspace with nothing registered yet — and BOTH can resolve
			// themselves with no further input from the user. Reusing
			// `pending_repo_selections` with an empty `options` array puts this
			// on the exact same recovery path `resumeHeldSelection` already has
			// for a corrupt pending row: the next signal on this session (a
			// follow-up prompt, or the delegator re-mentioning as the message
			// above suggests) replays THIS SAME webhook through `routeCreated`,
			// re-resolving against the then-current registry — instead of only
			// the user's literal next words being routed with no repository
			// decision at all, and the original delegation's own content lost.
			// If nothing ever arrives, the existing TTL sweep still expires the
			// row with an explicit "re-assign or mention me again" notice
			// instead of leaving it silent forever.
			this.store.createPendingRepoSelection({
				agentSessionId: sessionId,
				issueKey,
				workspaceId,
				options: [],
				createdEvent: JSON.stringify(webhook),
				createdMs: this.now(),
			});
			return "held";
		}

		const options = outcome.candidates.map((repo) => repo.name);
		if (!this.postRepositorySelection) {
			this.logger.warn(
				`Repository selection needed for ${issueKey} but no elicitation transport is configured; routing to a fallback`,
			);
			const fallback = resolver.fallbackDecision(outcome.candidates);
			if (!fallback) return "held";
			this.persistDecision(issueKey, fallback);
			return "ready";
		}

		try {
			await this.postRepositorySelection(
				workspaceId,
				sessionId,
				REPOSITORY_SELECTION_PROMPT,
				options,
			);
		} catch (error) {
			// Never stash a held event for an elicitation that was never posted:
			// nothing would ever arrive to release it, and the issue would sit
			// silently forever. Fall back instead, and say so.
			this.logger.error(
				`Failed to post the repository selection for ${issueKey}; routing to a fallback`,
				error,
			);
			const fallback = resolver.fallbackDecision(outcome.candidates);
			if (!fallback) return "held";
			this.persistDecision(issueKey, fallback);
			return "ready";
		}

		this.store.createPendingRepoSelection({
			agentSessionId: sessionId,
			issueKey,
			workspaceId,
			options,
			createdEvent: JSON.stringify(webhook),
			createdMs: this.now(),
		});
		this.logger.info(
			`Posted a repository selection for ${issueKey} (${outcome.reason}) with options [${options.join(", ")}]; holding the created event`,
		);
		return "held";
	}

	/**
	 * Drops any override that fails {@link BASE_BRANCH_RE} before it can ever be
	 * stored.
	 *
	 * `decision.baseBranchOverrides` originates in an issue description's
	 * `[repo=name#branch]` tag (`parseRepoTags`), not from the registry, so it
	 * never passes through `validateRegisteredRepository` — the gate that was
	 * added specifically for this shell sink. It is persisted here, read back
	 * into `RepositoryConfig.baseBranch`, and interpolated into a double-quoted
	 * shell string in `GitService.ts`
	 * (`` execSync(`git ls-remote --heads origin "${baseBranch}"`) ``) inside a
	 * user's worker container. `parseRepoTags`' charset has no shell
	 * metacharacters and no `=`, so this is not a live shell-escape exploit, but
	 * a leading `-` (git ref treated as an option) and `..` (path traversal in
	 * the ref name) are both expressible through the tag syntax and are exactly
	 * the two classes `BASE_BRANCH_RE` exists to exclude. Filtering here, at the
	 * one place every decision is persisted, means nothing invalid ever reaches
	 * `CYRUS_REPOS_JSON` — cheaper than validating again at every read site.
	 */
	private sanitizeBaseBranchOverrides(
		issueKey: string,
		overrides: Record<string, string>,
	): Record<string, string> {
		const sanitized: Record<string, string> = {};
		for (const [repoName, branch] of Object.entries(overrides)) {
			if (BASE_BRANCH_RE.test(branch)) {
				sanitized[repoName] = branch;
			} else {
				this.logger.warn(
					`Dropping invalid base branch override ${JSON.stringify(branch)} for ${repoName} on issue ${issueKey}: does not match ${BASE_BRANCH_RE}`,
				);
			}
		}
		return sanitized;
	}

	private persistDecision(
		issueKey: string,
		decision: RepositoryDecision,
	): void {
		this.store.setIssueRepositories(
			issueKey,
			{
				repoNames: decision.repositories.map((repo) => repo.name),
				baseBranchOverrides: this.sanitizeBaseBranchOverrides(
					issueKey,
					decision.baseBranchOverrides,
				),
				method: decision.method,
			},
			this.now(),
		);
		this.logger.info(
			`Repositories for ${issueKey}: [${decision.repositories
				.map((repo) => repo.name)
				.join(", ")}] (${decision.method})`,
		);
	}

	/**
	 * Ensures the issue's workspace image exists before anything boots.
	 *
	 * Returns `"held"` only while a build is running — the `created` webhook is
	 * stashed verbatim and replayed by
	 * {@link releaseHeldDevcontainerBuilds} when the build reaches a terminal
	 * state. Every other outcome, including a failed build, returns `"ready"`:
	 * the default worker image is a real fallback (it carries every toolchain),
	 * and holding an issue forever because its devcontainer does not compile
	 * would be worse than starting in an environment we can name.
	 */
	private async ensureDevcontainerImage(
		webhook: SessionEvent,
		issueKey: string,
		workspaceId: string,
	): Promise<"ready" | "held"> {
		const devcontainers = this.devcontainers;
		if (!devcontainers) return "ready";
		const sessionId = webhook.agentSession.id;

		if (this.store.getPendingDevcontainerBuild(sessionId)) {
			// A repeated `created` delivery for a session already waiting on a
			// build. Asking again would post a second notice and overwrite the
			// held event with an identical one.
			return "held";
		}

		const repositories = await this.repositoriesForIssue(issueKey, workspaceId);
		if (repositories.length === 0) return "ready";
		if (repositories.length > 1) {
			// Task 8 is blocked on an open question (which environment wins when
			// an issue deliberately fans out). The default worker image is the
			// plan's own recommended answer, and it is the only one that is safe
			// without asking — so take it, and say what it costs.
			await this.postActivityQuietly(
				workspaceId,
				sessionId,
				fillTemplate(DEVCONTAINER_MULTI_REPO_MESSAGE, {
					repositories: repositories.map((repo) => repo.name).join(", "),
				}),
			);
			return "ready";
		}

		const repo = repositories[0];
		if (!repo) return "ready";
		const outcome = await devcontainers.ensureForIssue(issueKey, repo);
		switch (outcome.kind) {
			case "default":
				return "ready";
			case "ready":
				if (this.devcontainerNotified.delete(issueKey)) {
					await this.postActivityQuietly(
						workspaceId,
						sessionId,
						fillTemplate(DEVCONTAINER_READY_MESSAGE, {
							repository: outcome.repositoryName,
						}),
					);
				}
				return "ready";
			case "failed":
				this.devcontainerNotified.delete(issueKey);
				this.logger.error(
					`Devcontainer image for ${issueKey} is unavailable; falling back to the default worker image: ${outcome.reason}`,
				);
				await this.postActivityQuietly(
					workspaceId,
					sessionId,
					fillTemplate(DEVCONTAINER_BUILD_FAILED_MESSAGE, {
						repository: repo.name,
						detail: outcome.runId
							? `ACR run ${outcome.runId}. ${outcome.reason}`
							: outcome.reason,
					}),
				);
				return "ready";
			case "building": {
				if (!this.devcontainerNotified.has(issueKey)) {
					this.devcontainerNotified.add(issueKey);
					await this.postActivityQuietly(
						workspaceId,
						sessionId,
						fillTemplate(DEVCONTAINER_BUILDING_MESSAGE, {
							repository: outcome.repositoryName,
						}),
					);
				}
				this.store.createPendingDevcontainerBuild({
					agentSessionId: sessionId,
					issueKey,
					workspaceId,
					cacheKey: outcome.cacheKey,
					createdEvent: JSON.stringify(webhook),
					createdMs: this.now(),
				});
				this.logger.info(
					`Holding the created event for ${issueKey} while ${outcome.repositoryName}'s workspace image builds`,
				);
				return "held";
			}
		}
	}

	/**
	 * Replays every `created` webhook held on a build that has just finished.
	 *
	 * The rows are taken in one transaction, so a build finishing can never
	 * replay the same event twice — and the replay goes back through
	 * {@link routeCreated}, which re-runs the gate and now finds a `ready` (or
	 * `failed`) cache row instead of a `building` one.
	 */
	private async releaseHeldDevcontainerBuilds(cacheKey: string): Promise<void> {
		for (const pending of this.store.takePendingDevcontainerBuilds(cacheKey)) {
			let held: SessionEvent | undefined;
			try {
				held = JSON.parse(pending.createdEvent) as SessionEvent;
			} catch (error) {
				this.logger.error(
					`Held created event for ${pending.issueKey} is unreadable; the delegation for this issue is lost`,
					error,
				);
				continue;
			}
			try {
				await this.routeCreated(held);
			} catch (error) {
				// `route()` is invoked as `void route(event)` with no catch, and
				// this runs from a build's completion callback rather than from a
				// webhook — so letting this escape would take the router process
				// down for every teammate over one failed replay.
				this.logger.error(
					`Could not replay the held created event for ${pending.issueKey} after its workspace image finished building`,
					error,
				);
			}
		}
	}

	/**
	 * The repositories an issue was routed to, as registered entries.
	 *
	 * Reads the decision the repository gate has already persisted; an issue
	 * with no decision (or one naming repositories since deregistered) gets an
	 * empty list, which the caller treats as "use the default image" rather
	 * than guessing at an environment.
	 */
	private async repositoriesForIssue(
		issueKey: string,
		workspaceId: string,
	): Promise<RegisteredRepository[]> {
		const registry = this.repositoryRegistry;
		const decision = this.store.getIssueRepositories(issueKey);
		if (!registry || !decision) return [];
		let all: RegisteredRepository[];
		try {
			({ repositories: all } = await registry.list());
		} catch (error) {
			this.logger.warn(
				`Could not read the repository registry to pick an environment for ${issueKey}; using the default worker image`,
				error,
			);
			return [];
		}
		const byName = new Map(all.map((repo) => [repo.name, repo]));
		return decision.repoNames
			.map((name) => byName.get(name))
			.filter(
				(repo): repo is RegisteredRepository =>
					repo !== undefined && repo.linearWorkspaceId === workspaceId,
			);
	}

	/**
	 * A Linear 5xx while posting build progress must never abort the gate: the
	 * build is the point, the notice is courtesy.
	 */
	private async postActivityQuietly(
		workspaceId: string,
		sessionId: string,
		body: string,
	): Promise<void> {
		try {
			await this.postActivity(workspaceId, sessionId, body);
		} catch (error) {
			this.logger.warn(
				`Failed to post a devcontainer progress activity for session ${sessionId}`,
				error,
			);
		}
	}

	private async routeCreated(webhook: SessionEvent): Promise<void> {
		const sessionId = webhook.agentSession.id;
		const workspaceId = webhook.organizationId;
		const issueId =
			webhook.agentSession.issueId ??
			webhook.agentSession.issue?.id ??
			undefined;
		const creator = webhook.agentSession.creator ?? undefined;

		// Repository selection happens here, on the router, before any container
		// device exists. `extractIssueKey` is the same gate the container path
		// uses; without a key there is no per-issue sandbox to route to and the
		// existing invalid-key handling below reports it.
		//
		// This deliberately runs BEFORE routability is known at all: an
		// unenrolled creator's issue gets asked "which repository?" before
		// `resolveTargetOrInvalidKey` below would ever discover there's no
		// device to route to and post `UNENROLLED_CREATOR_MESSAGE` instead. Do
		// not "fix" this by moving the gate after target resolution — the whole
		// point of asking here is to do it before ANY container-related work,
		// and target resolution is downstream of that. An unenrolled creator
		// answering the elicitation is harmless: `resumeHeldSelection`'s replay
		// re-runs this same gate and then hits the identical unenrolled-creator
		// path it would have hit originally.
		//
		// A KNOWN physical-device creator is different, and is excluded by
		// `isKnownPhysicalDeviceCreator` below: their own EdgeWorker already
		// runs its own `RepositoryRouter`/pending-selection flow (design doc
		// §5), so asking here too would double-elicit them — once from the
		// router, once from their device, for the same issue. `containers`
		// being configured (which is what gates `this.repositoryResolver` being
		// set at all — see RouterServer) only rules out deployments with NO
		// container path whatsoever; it does nothing for a MIXED deployment
		// that also has enrolled physical-device users, which is why this
		// second, per-creator check is required on top of it.
		// `isKnownPhysicalDeviceCreator` only covers the CREATOR field, not
		// affinity already established for this issue — see that method's doc
		// comment for the precedence gap this leaves open (M-1, deferred).
		const issueKey = extractIssueKey(webhook);
		if (
			issueKey !== undefined &&
			this.repositoryResolver &&
			!this.isKnownPhysicalDeviceCreator(creator)
		) {
			const gate = await this.ensureRepositoryDecision(
				webhook,
				issueKey,
				workspaceId,
				issueId,
			);
			if (gate === "held") return;
		}

		// The environment gate runs AFTER the repository gate and before any
		// target resolution, for the same reason: a build costs minutes, and
		// paying them with the webhook held is strictly better than creating a
		// device row and a sandbox for an issue whose image does not exist yet.
		if (
			issueKey !== undefined &&
			this.devcontainers &&
			!this.isKnownPhysicalDeviceCreator(creator)
		) {
			const gate = await this.ensureDevcontainerImage(
				webhook,
				issueKey,
				workspaceId,
			);
			if (gate === "held") return;
		}

		const { target, invalidIssueKey } = this.resolveTargetOrInvalidKey(
			webhook,
			sessionId,
			issueId,
			creator,
		);
		if (invalidIssueKey !== undefined) {
			await this.postActivity(
				workspaceId,
				sessionId,
				fillTemplate(INVALID_ISSUE_KEY_MESSAGE, { issueKey: invalidIssueKey }),
			);
			emitRoutingRejection(this.logger, {
				attribution: this.rejectionAttribution(webhook, sessionId),
				reason: "invalid_issue_key",
				sessionId,
				...(issueId !== undefined ? { issueId } : {}),
				issueKey: invalidIssueKey,
			});
			this.logger.warn(
				`Refused to route session ${sessionId}: issue key ${JSON.stringify(invalidIssueKey)} can't be used for a container workspace`,
			);
			return;
		}
		if (!target) {
			const userName = creator?.name ?? creator?.email ?? "there";
			await this.postActivity(
				workspaceId,
				sessionId,
				fillTemplate(UNENROLLED_CREATOR_MESSAGE, { userName }),
			);
			emitRoutingRejection(this.logger, {
				attribution: this.rejectionAttribution(webhook, sessionId),
				reason: "unenrolled_creator",
				sessionId,
				...(issueId !== undefined ? { issueId } : {}),
				...(issueKey !== undefined ? { issueKey } : {}),
			});
			this.logger.warn(
				`No enrolled Cyrus device for creator of session ${sessionId}`,
			);
			return;
		}

		// Issue lock (created events only). A different session already holding
		// the issue rejects this one.
		if (this.config.issueLock && issueId !== undefined) {
			if (!this.store.acquireIssueLock(issueId, sessionId, target.deviceId)) {
				// Read the holder BEFORE posting: this is the one fact that turns
				// "a comment did not reach an agent" from a guess into something an
				// operator can act on, and it is what tells them whether the holder
				// is a live session (reply in its thread) or a strand that needs
				// `cyrus router unlock`.
				const holder = this.store.getIssueLock(issueId);
				// Which message depends on WHOSE session holds the lock, because
				// the recovery differs and the wrong one is a closed loop: with
				// `creatorOnlyPrompting` on (the default), telling a non-creator to
				// "reply in the running session's thread" sends them into a gate
				// that rejects them and tells them to start their own session,
				// which lands back here. Only claim same-user recovery when we can
				// positively establish it — an unknown holder creator falls back to
				// the neutral message rather than promising something that fails.
				const holderCreator = holder
					? this.storedCreator(holder.sessionId)
					: undefined;
				const lockedByOther =
					this.config.creatorOnlyPrompting &&
					holderCreator?.id !== undefined &&
					creator?.id !== undefined &&
					holderCreator.id !== creator.id;
				await this.postActivity(
					workspaceId,
					sessionId,
					lockedByOther
						? fillTemplate(ISSUE_LOCKED_BY_OTHER_MESSAGE, {
								holderName:
									holderCreator?.name ?? holderCreator?.email ?? "someone else",
							})
						: ISSUE_LOCKED_MESSAGE,
				);
				emitRoutingRejection(this.logger, {
					attribution: this.rejectionAttribution(webhook, sessionId),
					reason: "issue_locked",
					sessionId,
					issueId,
					...(issueKey !== undefined ? { issueKey } : {}),
					...(holder ? { heldBySessionId: holder.sessionId } : {}),
					...(holder ? { heldByDeviceId: holder.deviceId } : {}),
				});
				// WARN, not the INFO it was. A rejection here makes the ISSUE
				// unreachable — the user's prompt is answered only inside the
				// abandoned session's own thread, which nobody is reading — and it
				// sat at INFO among 220 near-identical "ignoring non-agent-session
				// webhook" lines while CAN-133 was write-only for 5h17m (NOR-402).
				this.logger.warn(
					`Issue ${issueId} is already locked by session ${holder?.sessionId ?? "unknown"} ` +
						`(device ${holder?.deviceId ?? "unknown"}); rejected session ${sessionId}. ` +
						`The prompt did NOT reach an agent: the reply is posted only in the rejected ` +
						`session's own thread. If the holding session is no longer working, it is ` +
						`stranded — see sandbox.stranded_session — and needs \`cyrus router unlock\`.`,
				);
				return;
			}
		}

		this.store.setSessionAffinity(
			sessionId,
			target.deviceId,
			creator ? JSON.stringify(creator) : undefined,
		);
		if (issueId !== undefined) {
			this.store.setIssueAffinity(issueId, target.deviceId);
		}
		this.sessionWorkspace.set(sessionId, workspaceId);

		// Accepting an event must be at least as visible as ignoring one. Every
		// rejection path above logs, and so does every webhook the router
		// deliberately drops — but until this line the SUCCESS path was silent,
		// so a console showing only "ignoring non-agent-session webhook" lines
		// looked exactly like an agent-session event that never arrived.
		this.logger.info(
			`Routed session ${sessionId} (issue ${issueId ?? "unknown"}) to ${
				target.kind
			} device ${target.deviceId}`,
		);

		await this.deliverOrNotify(webhook, target, sessionId, workspaceId);

		// Delivery first, promotion second: the device only needs the queued event
		// to start work, and promotion costs several Linear round trips.
		if (issueId !== undefined) {
			await this.promoteIssue(workspaceId, issueId);
		}
	}

	/**
	 * The canonical attribution to stamp on a routing REJECTION.
	 *
	 * Prefers the session's existing run, so a prompt rejected against a session
	 * that has already been routed carries the same workspace/owner/team/project
	 * columns as that run's own `run.routed` line — which is what lets one
	 * `where p["cyrus.session_id"] == …` return the whole story instead of two
	 * halves that have to be joined by hand.
	 *
	 * Falls back to the webhook's own snapshot for a session that never got a
	 * run, which is the common case: most rejections happen before one exists.
	 * Everything the snapshot cannot supply stays null. Notably the OWNER: a
	 * rejection for an unenrolled creator has, by definition, no Cyrus user to
	 * attribute to, and inventing one from the webhook's actor would put a
	 * non-existent owner into the column every ownership filter reads.
	 */
	private rejectionAttribution(
		webhook: SessionEvent,
		sessionId: string,
	): RunAttribution {
		const run = this.store.getAgentRunForSession(sessionId);
		return run
			? runAttribution(run)
			: routingAttribution(extractRoutingSnapshot(webhook));
	}

	/**
	 * Emit one run-lifecycle event, reading the run back so the attributes are
	 * the row's, not the caller's idea of it.
	 *
	 * The read-back matters: `recordAgentRunRouted` and `finishAgentRun` both
	 * merge into an existing row, so the facts on the emitted line (the runner a
	 * terminal frame just supplied, the project a prior enrichment filled in) are
	 * only correct after the write. A no-op when the run cannot be found —
	 * a lifecycle event with no run to attribute is not worth a guessed one.
	 *
	 * `changed` is the store's report of whether it actually transitioned a row,
	 * and passing it is MANDATORY on the ending paths. Both `finishAgentRun` and
	 * `markAgentRunUnknown` return early on a run that is already terminal, which
	 * is routine rather than exceptional: `routePrompted` re-establishes affinity
	 * for an already-terminal session, and `RouterServer` applies a
	 * `session_state` frame before acking it so a device replays frames the router
	 * already applied. Emitting regardless would file a healthy completed run
	 * under `run.unknown` — "nobody saw the outcome" — which is precisely the
	 * misdiagnosis the `finished`/`unknown` split exists to prevent, and would
	 * double-count the same run id under two different outcomes.
	 */
	private emitRunLifecycle(
		sessionId: string,
		name: RunEventName,
		extra?: LogEventAttributes,
		changed = true,
	): void {
		if (!changed) return;
		const run = this.store.getAgentRunForSession(sessionId);
		if (!run) return;
		emitRunEvent(this.logger, name, runAttribution(run), extra);
	}

	/**
	 * Fills the routing-snapshot dimensions the webhook could not supply.
	 *
	 * Entirely best-effort, on every axis. It returns early when the snapshot is
	 * already complete (so it is one Linear read per run, not per webhook), the
	 * read itself is allowed to fail, and the store fills only blanks — so a
	 * dimension captured at route time can never be rewritten by a later read of
	 * an issue that has since moved.
	 *
	 * A failure here costs a filter dimension on one run. It must never be
	 * allowed to fail a route, which is why the caller does not await it and why
	 * everything below is swallowed.
	 */
	private async enrichRunRouting(
		runId: string,
		workspaceId: string,
		issueId: string,
	): Promise<void> {
		if (!this.fetchRoutingContext) return;
		try {
			if (!this.store.runRoutingNeedsEnrichment(runId)) return;
			const context = await this.fetchRoutingContext(workspaceId, issueId);
			// Stamped even for an empty answer — an issue in no project is a
			// complete answer, and leaving the run un-stamped would re-ask on every
			// subsequent input. A read that THREW is deliberately not stamped: the
			// next input retries it.
			if (context) this.store.enrichRunRouting(runId, context, this.now());
		} catch (error) {
			this.logger.warn(
				`Could not complete the routing snapshot for run ${runId} (issue ${issueId}); its team/project filters will be incomplete`,
				error,
			);
		}
	}

	/**
	 * Marks a delegated issue as started in Linear. Reached only once the event
	 * has been accepted — an unenrolled creator or a lock rejection returns from
	 * {@link routeCreated} before this, so a rejected issue is never promoted.
	 *
	 * Best-effort: a Linear failure here must not fail the routing that already
	 * succeeded, so it is logged and swallowed.
	 */
	private async promoteIssue(
		workspaceId: string,
		issueId: string,
	): Promise<void> {
		if (!this.moveIssueToStartedState) return;
		try {
			const stateName = await this.moveIssueToStartedState(
				workspaceId,
				issueId,
			);
			if (stateName !== undefined) {
				this.logger.info(`Moved issue ${issueId} to '${stateName}'`);
			}
		} catch (err) {
			this.logger.warn(
				`Failed to move issue ${issueId} to a started state`,
				err,
			);
		}
	}

	/**
	 * Consumes a prompt that answers a pending repository selection.
	 *
	 * Returns `true` when the prompt was the answer and this webhook has been
	 * fully handled, `false` when there was no pending selection and normal
	 * prompt routing should continue.
	 *
	 * Two shapes of answer, both terminal:
	 *  - the body names an offered option -> that repository is the decision, and
	 *    the HELD `created` event is replayed so the runner initialises from the
	 *    delegation. The answer itself is consumed: delivering "cyrus-web" as a
	 *    user prompt would start the session with a repository name as its task.
	 *  - anything else -> the user ignored the question. Fall back, then deliver
	 *    the held `created` event AND this prompt, which is the semantics device
	 *    mode already has (see packages/CLAUDE.md).
	 */
	private async resumeHeldSelection(webhook: SessionEvent): Promise<boolean> {
		const resolver = this.repositoryResolver;
		if (!resolver) return false;
		const sessionId = webhook.agentSession.id;
		const pending = this.store.getPendingRepoSelection(sessionId);
		if (!pending) return false;

		// Creator-only prompting MUST gate here, before this method does
		// anything else — not only after, the way `routePrompted`'s own
		// creator check runs today. Acting first (persisting a decision,
		// replaying the held delegation, and booting a container) cannot be
		// undone by a rejection that only fires afterward; by then a real
		// session is already running on the CREATOR's device, started by
		// someone else's message. This deviates from the brief's literal
		// "first statement of routePrompted" ordering deliberately: the
		// creator-only invariant wins. `agentSession.creator` is used (not the
		// stored session-affinity creator `routePrompted` normally compares
		// against) because a held session has never had affinity established —
		// there is nothing stored yet to compare against — and
		// `agentSession.creator` is always the session's ORIGINAL creator
		// regardless of who is prompting now, exactly as `routePrompted`'s own
		// comment on this documents. Answering is refused outright (rather than
		// falling through to `routePrompted`'s normal target resolution, which
		// would run first and could mint a container device as a side effect of
		// `ensureDevice` even though the prompt is rejected a few lines later);
		// the pending row is left completely untouched so the real creator can
		// still answer it.
		if (this.config.creatorOnlyPrompting) {
			const creatorId = webhook.agentSession.creator?.id;
			const actorId = webhook.agentActivity?.userId ?? undefined;
			if (
				creatorId !== undefined &&
				(actorId === undefined || actorId !== creatorId)
			) {
				await this.postActivity(
					webhook.organizationId,
					sessionId,
					PROMPT_REJECTION_MESSAGE,
				);
				emitRoutingRejection(this.logger, {
					attribution: this.rejectionAttribution(webhook, sessionId),
					reason: "non_creator_prompt",
					sessionId,
				});
				this.logger.warn(
					`Rejected a non-creator's answer to the repository selection for session ${sessionId} (actor ${actorId ?? "unknown"} != creator ${creatorId}); the pending selection is untouched`,
				);
				return true;
			}
			// else: creatorId is unknown (no creator on the webhook at all) —
			// nothing to compare against, so fall through exactly as
			// `routePrompted`'s own gate does in that case.
		}

		// A stop signal (or the literal "stop"-shaped body the device itself
		// treats the same way — see EdgeWorker's `isTextStopRequest` and
		// packages/CLAUDE.md's "checked FIRST, before any routing work") means
		// the user wants to abandon this delegation, not answer it. Nothing has
		// booted yet — that is the entire point of holding — so there is no
		// running session to stop. Treat it purely as an abandonment: drop the
		// pending selection and boot nothing, rather than falling through to
		// the unrelated-reply fallback below, which would persist a decision,
		// replay the held `created` event, boot a container, and only then
		// deliver a stop to a session that had just started.
		if (isStopRequest(webhook)) {
			this.store.deletePendingRepoSelection(sessionId);
			this.logger.info(
				`Session ${sessionId} was stopped while its repository selection was pending; abandoned the selection without booting`,
			);
			return true;
		}

		if (pending.options.length === 0) {
			// Two DIFFERENT origins land here with the identical stored shape,
			// and both get the identical recovery: (1) `RouterStore
			// .getPendingRepoSelection` degrades a corrupt `options_json` to an
			// empty array rather than throwing — a real, documented state, not a
			// hypothetical; (2) `ensureRepositoryDecision`'s "unavailable"
			// branch deliberately stores an empty-options row too, for a
			// registry that was transiently unreadable or had nothing
			// registered yet at `created` time. Neither case has options left to
			// honor an answer against, but the row still carries a readable
			// `createdEvent`: replay it through the normal creation gate (which
			// re-resolves, or re-asks, from the live registry) instead of
			// silently discarding the delegation. Losing the held event here
			// would strand the issue with no way back, which is worse than
			// re-asking (or re-attempting) once more. The stale row is deleted
			// BEFORE the replay (unlike the normal path below) because
			// `ensureRepositoryDecision` treats an existing pending row as "already
			// asked, don't re-resolve" — leaving this one in place would make the
			// replay a no-op and the issue would stay stuck behind it forever.
			this.store.deletePendingRepoSelection(sessionId);
			this.logger.warn(
				`Pending repository selection for ${pending.issueKey} has no options to offer (registry was unavailable/empty at creation time, or the row is corrupt); replaying the held event through normal resolution instead of guessing`,
			);
			let held: SessionEvent | undefined;
			try {
				held = JSON.parse(pending.createdEvent) as SessionEvent;
			} catch (error) {
				this.logger.error(
					`Held created event for ${pending.issueKey} is ALSO unreadable; the delegation for this issue is lost`,
					error,
				);
			}
			if (held) {
				await this.routeCreated(held);
				// The replay may have landed back in a held state itself — tied
				// again into a fresh elicitation, or blocked because nothing is
				// registered for the workspace. Either way this session is STILL
				// waiting on a repository choice, so the current prompt must not
				// fall through to normal routing: that would resolve a target and
				// boot a container while the question is still open, delivering a
				// stale prompt to a sandbox that only just started (and, for a
				// re-tied selection, would leave the user staring at a duplicate
				// "which repository?" prompt on top of an already-running
				// container). Consume it here instead.
				const stillUnresolved =
					this.store.getPendingRepoSelection(sessionId) !== undefined ||
					this.store.getIssueRepositories(pending.issueKey) === undefined;
				if (stillUnresolved) return true;
			}
			return false;
		}

		// Deleted synchronously here — before the first `await` below — rather
		// than after the replay has gone through. Two distinct prompts answering
		// the SAME held selection both call `getPendingRepoSelection` with no
		// `await` in between (every check above this line is synchronous), so
		// only the first to reach this statement still finds a row: the second's
		// read (whether concurrent or merely reordered by the event loop once the
		// first one awaits) sees it already gone and returns `false` at the top
		// of this method instead of persisting a second decision and replaying
		// the held `created` event a second time. Matches the precedent the
		// empty-options branch above already set, for the identical reason.
		this.store.deletePendingRepoSelection(sessionId);

		const { repositories } = await resolver
			.resolve({ workspaceId: pending.workspaceId, issueId: undefined })
			.then((outcome) =>
				outcome.kind === "needs_selection"
					? { repositories: outcome.candidates }
					: { repositories: [] },
			)
			.catch(() => ({ repositories: [] }));

		// Prefer the exact options that were offered; `repositories` is only a
		// safety net for a registry that changed while the user was deciding.
		const candidates =
			repositories.length > 0
				? repositories.filter((repo) => pending.options.includes(repo.name))
				: [];
		const offered =
			candidates.length > 0
				? candidates
				: pending.options.map((name) => ({
						name,
						githubSlug: "",
						linearWorkspaceId: pending.workspaceId,
					}));

		const body = webhook.agentActivity?.content?.body ?? "";
		const selected = resolver.selectByOptionValue(body, offered);
		const decision = selected ?? resolver.fallbackDecision(offered);

		if (!decision) {
			// Defensively unreachable: `offered` is guaranteed non-empty here
			// (built from `pending.options`, already known non-empty above), so
			// `fallbackDecision` can never return undefined. Kept anyway per this
			// file's existing defensive style. The row was already deleted above,
			// so there is nothing left to clean up here.
			this.logger.warn(
				`Pending repository selection for ${pending.issueKey} could not be resolved; dropping it`,
			);
			return false;
		}
		this.persistDecision(pending.issueKey, decision);

		// Replay the held delegation first, so the container's first event is the
		// one that starts a session.
		let held: SessionEvent | undefined;
		try {
			held = JSON.parse(pending.createdEvent) as SessionEvent;
		} catch (error) {
			// What happens to the CURRENT prompt from here depends on `selected`,
			// computed above: a matched answer is consumed (the method returns
			// `true` below and the prompt is never routed), so only the
			// unrelated-reply case actually falls through to normal prompt
			// routing. Say which one this is, rather than always claiming the
			// prompt is routed.
			this.logger.error(
				`Held created event for ${pending.issueKey} is unreadable; ${
					selected
						? "the repository selection was still applied, but the delegation itself is lost"
						: "routing the prompt alone"
				}`,
				error,
			);
		}
		if (held) {
			try {
				await this.routeCreated(held);
			} catch (error) {
				// The row was already deleted above (to close the concurrent-prompt
				// race — see the comment there), so a failed replay can no longer
				// lean on "the row is still there" the way the pre-fix code did to
				// avoid stranding the delegation; moving the delete earlier without
				// this catch would have silently reintroduced that exact loss.
				// Re-stash the pending row verbatim (same options, same held event)
				// instead, so the NEXT signal on this session replays it again
				// through this same method — the identical recovery path
				// `ensureRepositoryDecision`'s "unavailable" branch already uses for
				// a registry that was unreadable at `created` time. Re-deriving the
				// decision persisted just above on that retry is idempotent and
				// harmless. The error is swallowed rather than rethrown: `route()`
				// is invoked as `void route(event)` with no catch (see
				// `claimWebhook`'s doc comment), so letting this escape would take
				// the whole router process down for every teammate over one failed
				// replay.
				this.logger.error(
					`Failed to replay the held delegation for ${pending.issueKey} after a repository selection; re-stashing it for retry`,
					error,
				);
				this.store.createPendingRepoSelection({
					agentSessionId: sessionId,
					issueKey: pending.issueKey,
					workspaceId: pending.workspaceId,
					options: pending.options,
					createdEvent: pending.createdEvent,
					createdMs: pending.createdMs,
				});
				return true;
			}
		}

		if (selected) {
			this.logger.info(
				`Session ${sessionId} selected repository ${decision.repositories[0]?.name}`,
			);
			return true;
		}

		this.logger.info(
			`Session ${sessionId} answered the repository selection with an unrelated prompt; used ${decision.repositories[0]?.name} and forwarding the prompt`,
		);
		return false;
	}

	private async routePrompted(webhook: SessionEvent): Promise<void> {
		const sessionId = webhook.agentSession.id;
		if (await this.resumeHeldSelection(webhook)) return;
		const workspaceId = webhook.organizationId;
		const issueId =
			webhook.agentSession.issueId ??
			webhook.agentSession.issue?.id ??
			undefined;
		const creator = webhook.agentSession.creator ?? undefined;

		// A prompt must resolve through the SAME chain as a created event, not
		// through session affinity alone. Affinity is deleted the moment a session
		// reports a terminal state, but a Linear agent session outlives its turns:
		// the user can always prompt it again. Resolving on affinity only meant
		// every follow-up prompt after the first completion was dropped, leaving
		// the session in "Waiting for Cyrus" forever.
		const { target, invalidIssueKey } = this.resolveTargetOrInvalidKey(
			webhook,
			sessionId,
			issueId,
			creator,
		);
		if (invalidIssueKey !== undefined) {
			await this.postActivity(
				workspaceId,
				sessionId,
				fillTemplate(INVALID_ISSUE_KEY_MESSAGE, { issueKey: invalidIssueKey }),
			);
			emitRoutingRejection(this.logger, {
				attribution: this.rejectionAttribution(webhook, sessionId),
				reason: "invalid_issue_key",
				sessionId,
				...(issueId !== undefined ? { issueId } : {}),
				issueKey: invalidIssueKey,
			});
			this.logger.warn(
				`Refused to route prompted session ${sessionId}: issue key ${JSON.stringify(invalidIssueKey)} can't be used for a container workspace`,
			);
			return;
		}
		if (!target) {
			await this.postActivity(
				workspaceId,
				sessionId,
				PROMPT_UNROUTABLE_MESSAGE,
			);
			emitRoutingRejection(this.logger, {
				attribution: this.rejectionAttribution(webhook, sessionId),
				reason: "prompt_unroutable",
				sessionId,
				...(issueId !== undefined ? { issueId } : {}),
			});
			this.logger.warn(
				`Prompted event for session ${sessionId} resolved to no device; notified and dropping`,
			);
			return;
		}
		const deviceId = target.deviceId;

		if (this.config.creatorOnlyPrompting) {
			// Reference creator: the stored one, else the session creator carried on
			// the webhook. The webhook's `agentSession.creator` is ALWAYS the
			// session's original creator, which is exactly what we want on the
			// *creator* side of this comparison — and it is the only thing we have
			// once a terminal state has cleared the stored affinity row. Without
			// this fallback, a session rescued by resolveTarget() above would have
			// no stored creator and the gate would silently skip, letting anyone
			// prompt someone else's finished session onto that person's machine.
			const creatorId = this.storedCreatorId(sessionId) ?? creator?.id;
			if (creatorId !== undefined) {
				// Actor of the prompt: ONLY the activity's own `userId` identifies
				// who is actually prompting right now. Do NOT fall back to
				// `agentSession.creator?.id` — that field is always the session's
				// original creator, regardless of who sent this prompt, so using it
				// as a fallback would let a non-creator's prompt masquerade as the
				// creator's whenever the activity omits `userId` (a fail-open bug).
				// Fail closed instead: an actor we can't positively identify is
				// rejected exactly like one we can identify as a mismatch.
				const actorId = webhook.agentActivity?.userId ?? undefined;
				if (actorId === undefined || actorId !== creatorId) {
					await this.postActivity(
						workspaceId,
						sessionId,
						PROMPT_REJECTION_MESSAGE,
					);
					// This is the refusal `ISSUE_LOCKED_MESSAGE` steers people
					// into: it tells a lock-rejected user to reply in the holding
					// session's thread, and if they are not that session's creator
					// this gate rejects them again. Left at `info` with no event,
					// that loop was invisible from both ends (NOR-402).
					emitRoutingRejection(this.logger, {
						attribution: this.rejectionAttribution(webhook, sessionId),
						reason: "non_creator_prompt",
						sessionId,
						...(issueId !== undefined ? { issueId } : {}),
					});
					this.logger.warn(
						`Rejected non-creator prompt on session ${sessionId} (actor ${actorId ?? "unknown"} != creator ${creatorId})`,
					);
					return;
				}
			}
			// else: creatorId is unknown — neither stored nor on the webhook (e.g. a
			// session routed via issue/parent affinity that never carried a creator).
			// There is nothing to compare the actor against, so the gate is
			// intentionally skipped and the prompt is allowed through — a deliberate
			// can't-compare-so-allow case, not an oversight.
		}

		// Re-establish affinity. When we got here via the fallback chain the row was
		// missing (or pointed at a device that has since been replaced), so writing
		// it back means the next prompt resolves on the fast path — and restores the
		// stored creator that the creator-only gate above compares against.
		this.store.setSessionAffinity(
			sessionId,
			deviceId,
			creator ? JSON.stringify(creator) : undefined,
		);
		if (issueId !== undefined) {
			this.store.setIssueAffinity(issueId, deviceId);
		}

		const email = webhook.agentSession.creator?.email ?? DEFAULT_EMAIL;
		this.sessionWorkspace.set(sessionId, workspaceId);
		await this.deliverOrNotify(
			webhook,
			{ ...target, deviceId, email },
			sessionId,
			workspaceId,
		);
	}

	/**
	 * Wraps {@link resolveTarget}, translating an {@link InvalidIssueKeyError}
	 * into a returned `invalidIssueKey` field instead of letting it propagate
	 * as an exception. Callers (`routeCreated`/`routePrompted`) use this to
	 * post the accurate "this issue's identifier can't be used" notice
	 * instead of treating a container-executor user as unenrolled or a prompt
	 * as unroutable (Finding 4). Any other error is not ours to translate and
	 * propagates unchanged.
	 */
	private resolveTargetOrInvalidKey(
		webhook: SessionEvent,
		sessionId: string,
		issueId: string | undefined,
		creator: SessionEvent["agentSession"]["creator"] | undefined,
	): { target: ResolvedTarget | undefined; invalidIssueKey?: string } {
		// The routing DECISION, spanned separately from the routing work around
		// it. It is fast — a handful of SQLite reads — so it is not here for its
		// duration; it is here because `cyrus.outcome` on this span is the single
		// attribute that answers "why did this issue go to that machine", which
		// is otherwise reconstructible only by reading four log lines in order.
		return withSpanSync(
			routerTracer(),
			ROUTER_SPANS.resolveTarget,
			{ attributes: cyrusSpanAttributes({ session_id: sessionId }) },
			(span) => {
				try {
					const target = this.resolveTarget(
						webhook,
						sessionId,
						issueId,
						creator,
					);
					span.setAttributes(
						cyrusSpanAttributes({
							outcome: target ? "resolved" : "unroutable",
							device_id: target?.deviceId,
							target_kind: target?.kind,
						}),
					);
					return { target };
				} catch (err) {
					if (err instanceof InvalidIssueKeyError) {
						// Not recorded as a span error: an unusable issue key is a
						// rejected input, not a router fault, and the caller turns it
						// into a user-facing message. Marking it red would put it in
						// the same bucket as a database failure.
						span.setAttributes(
							cyrusSpanAttributes({ outcome: "invalid_issue_key" }),
						);
						return { target: undefined, invalidIssueKey: err.issueKey };
					}
					throw err;
				}
			},
		);
	}

	/**
	 * Resolves the device an event routes to, in priority order:
	 * existing session affinity (re-delivery) -> creator's enrolled/container
	 * device -> issue affinity (app-created sub-issues) -> parent-issue
	 * affinity.
	 *
	 * Shared by created and prompted events so a prompt to a session whose
	 * affinity was released still reaches its owner's device.
	 *
	 * @throws {InvalidIssueKeyError} when the creator resolves to a
	 * container-executor user whose issue key fails the container service's
	 * safety gate. Callers should use {@link resolveTargetOrInvalidKey}
	 * rather than calling this directly, unless they intend to handle that
	 * exception themselves.
	 */
	private resolveTarget(
		webhook: SessionEvent,
		sessionId: string,
		issueId: string | undefined,
		creator: SessionEvent["agentSession"]["creator"] | undefined,
	): ResolvedTarget | undefined {
		const fallbackEmail = creator?.email ?? DEFAULT_EMAIL;

		const affinityDevice = this.store.getSessionAffinity(sessionId);
		if (affinityDevice !== undefined) {
			const info = this.store.getDeviceInfo(affinityDevice);
			if (info) {
				return {
					deviceId: affinityDevice,
					email: fallbackEmail,
					kind: info.kind,
					issueKey: info.issueKey,
				};
			}
			// Dangling affinity: the device row it pointed at is gone (e.g. its
			// container was destroyed and replaced under a different device
			// id). Clear it and fall through the chain below instead of
			// routing into the void.
			const ended = this.store.markAgentRunUnknown(sessionId, this.now());
			this.emitRunLifecycle(
				sessionId,
				RUN_EVENTS.unknown,
				{
					reason: "dangling_affinity" satisfies RunUnknownReason,
					reporting_device_id: affinityDevice,
				},
				ended,
			);
			this.store.clearSessionAffinity(sessionId);
			this.logger.warn(
				`Session ${sessionId} affinity pointed at deleted device ${affinityDevice}; clearing and re-resolving`,
			);
		}

		if (creator) {
			const user = this.store.findUserForCreator({
				id: creator.id,
				email: creator.email,
			});
			if (user) {
				const containerTargets = this.containerTargets;
				const provider = containerTargets?.executorFor(user.userId);
				if (containerTargets && provider) {
					try {
						// Fail CLOSED when the webhook carries no issue identifier —
						// do NOT fall back to `issueId`/`sessionId`. Both are Linear
						// internal UUIDs: they pass `ISSUE_KEY_RE` (alphanumeric +
						// dashes) just fine, so a silent fallback would happily mint
						// a container keyed by a UUID that can never round-trip. The
						// in-container edge worker always uploads its floor bundle
						// under `session.issue.identifier` (the human-readable key,
						// e.g. "CYPACK-11") — a UUID-keyed device means every upload
						// for that container's entire life 403s against the
						// artifact endpoint's issue-key scoping, silently. Treating
						// a missing identifier as an invalid issue key (rather than
						// a routable-but-wrong one) surfaces the same user-facing
						// notice as a malformed identifier instead of quietly
						// creating a container nothing can ever sync to.
						const issueKey = extractIssueKey(webhook);
						if (issueKey === undefined) {
							throw new InvalidIssueKeyError(issueId ?? sessionId);
						}
						const { deviceId } = containerTargets.ensureDevice(user, issueKey);
						return { deviceId, email: user.email, kind: "container", issueKey };
					} catch (err) {
						if (err instanceof InvalidIssueKeyError) {
							// Distinct from "can't route this at all": the user IS
							// enrolled with a container executor, but THIS issue's
							// identifier can't name a workspace. Propagate so the
							// caller can post the accurate message instead of
							// UNENROLLED_CREATOR_MESSAGE (Finding 4).
							throw err;
						}
						// Anything else (e.g. a store error): the container service is
						// the gate against a malformed issue key (or a store error)
						// ever reaching the store/provider — a user-facing "can't
						// route this" message is a safer failure mode than either
						// crashing the router or falling back silently to some other
						// device.
						this.logger.error(
							`Failed to resolve container device for ${user.email}`,
							err,
						);
						return undefined;
					}
				}
				const device = this.store.getDeviceForUser(user.userId);
				if (device) {
					return {
						deviceId: device.deviceId,
						email: user.email,
						kind: "device",
					};
				}
			}
		}

		if (issueId !== undefined) {
			const issueDevice = this.store.getIssueAffinity(issueId);
			if (issueDevice !== undefined) {
				const info = this.store.getDeviceInfo(issueDevice);
				if (info) {
					return {
						deviceId: issueDevice,
						email: fallbackEmail,
						kind: info.kind,
						issueKey: info.issueKey,
					};
				}
				// Dangling issue affinity: the device it pointed at is gone.
				// `revokeDevice` deletes the `devices` row WITHOUT calling
				// `purgeDeviceScopedRows`, and `issue_affinity.device_id` has no
				// FK cascade, so a live row can point at nothing. Heal it the
				// same way the session-affinity fast path above does — clear and
				// fall through — instead of returning a target that would blow
				// up in `enqueueEvent` ("Unknown device") and take the router
				// process down (Finding 3).
				this.store.clearIssueAffinity(issueId);
				this.logger.warn(
					`Issue ${issueId} affinity pointed at deleted device ${issueDevice}; clearing and re-resolving`,
				);
			}
		}

		const parentIssueId = extractParentIssueId(webhook);
		if (parentIssueId === undefined) {
			// Nothing else resolved the target above, so this fallback was our
			// last resort before falling through to UNENROLLED. Make the gap
			// visible: the typed webhook carries no parent-issue id today, so
			// this branch never actually fires (see extractParentIssueId).
			this.logger.info(
				"Parent-issue affinity fallback: webhook carries no parent issue id (app-attributed sub-issue affinity not implemented)",
			);
		}
		if (parentIssueId !== undefined) {
			const parentDevice = this.store.getIssueAffinity(parentIssueId);
			if (parentDevice !== undefined) {
				const info = this.store.getDeviceInfo(parentDevice);
				if (info) {
					return {
						deviceId: parentDevice,
						email: fallbackEmail,
						kind: info.kind,
						issueKey: info.issueKey,
					};
				}
				// Same healing as the issue-affinity branch above: a dangling
				// row must be cleared and fallen through, not returned as a
				// target (Finding 3).
				this.store.clearIssueAffinity(parentIssueId);
				this.logger.warn(
					`Parent issue ${parentIssueId} affinity pointed at deleted device ${parentDevice}; clearing and re-resolving`,
				);
			}
		}

		return undefined;
	}

	private deliverOrNotify(
		event: SessionEvent,
		target: ResolvedTarget,
		sessionId: string,
		workspaceId: string,
	): Promise<void> {
		return withSpan(
			routerTracer(),
			ROUTER_SPANS.dispatch,
			{
				// PRODUCER: this hands the event to a queue that another process
				// consumes. The matching CONSUMER span is the worker's, minted when
				// `RouterConnection` delivers the `event` frame — which is what makes
				// the two halves of the trace meet.
				kind: SpanKind.PRODUCER,
				attributes: cyrusSpanAttributes({
					device_id: target.deviceId,
					target_kind: target.kind,
					session_id: sessionId,
					workspace_id: workspaceId,
				}),
			},
			() => this.deliverOrNotifyInner(event, target, sessionId, workspaceId),
		);
	}

	private async deliverOrNotifyInner(
		event: SessionEvent,
		target: ResolvedTarget,
		sessionId: string,
		workspaceId: string,
	): Promise<void> {
		// Captured HERE, at enqueue time, and persisted with the row. The device
		// may be offline for minutes — a cold container boot, a suspended
		// sandbox — and `deliverPending` runs from a socket callback with no
		// relation to this call stack, so a context derived at send time would
		// attach the event to the wrong thing entirely.
		const routedMs = this.now();
		this.store.enqueueEvent(
			target.deviceId,
			JSON.stringify(event),
			routedMs,
			this.config.eventTtlMs,
			injectTraceContext(),
		);
		const input = extractRunInput(event, routedMs);
		const issueId =
			event.agentSession.issueId ?? event.agentSession.issue?.id ?? undefined;
		const runId = this.store.recordAgentRunRouted({
			deviceId: target.deviceId,
			issueKey: extractIssueKey(event) ?? target.issueKey ?? "unknown",
			...(issueId !== undefined ? { issueId } : {}),
			sessionId,
			// Captured HERE, from the webhook that caused the route, and written in
			// the same transaction that creates the run. A later query would read
			// whatever the issue has MOVED to, which is precisely what makes a
			// historical team or project filter unstable.
			routing: extractRoutingSnapshot(event),
			// Seeds the run's durable connectivity from the socket registry. Without
			// it, a run created on a device that was ALREADY connected has its
			// worker's state unobserved until that device next connects or drops —
			// for a long-lived physical device, potentially never.
			workerOnline: this.gateway.isOnline(target.deviceId),
			...input,
		});
		// The run's first appearance in the log stream, and the anchor every later
		// line about it is correlated to. Emitted BEFORE the enrichment below, so
		// its project columns can still be null — deliberately: this records what
		// was known when the route happened, and a line that waited for a Linear
		// round trip would no longer be that.
		this.emitRunLifecycle(sessionId, RUN_EVENTS.routed, {
			target_kind: target.kind,
			worker_online: this.gateway.isOnline(target.deviceId),
		});
		// The project is not on the webhook at any nesting, so it takes a Linear
		// read. Fire-and-forget and gated on the snapshot still being blank, so it
		// costs one round-trip per RUN and never delays delivery — the queued event
		// below is what actually starts the work.
		if (issueId !== undefined) {
			void this.enrichRunRouting(runId, workspaceId, issueId);
		}

		if (this.gateway.isOnline(target.deviceId)) {
			this.gateway.deliverPending(target.deviceId);
			return;
		}

		if (target.kind === "container") {
			// A container that isn't running yet is NOT an outage — cold-booting
			// it is the expected path, so no offlineWaitingMessage. boot() posts
			// its own (once-per-issue) failure notice only if ensureRunning
			// actually rejects; the queue drains once the container connects.
			//
			// Logged because this is the last thing the router does for a cold
			// start: everything after it happens inside the container. Without
			// this line, a worker that boots but never dials back is
			// indistinguishable from an event that was never dispatched.
			this.logger.info(
				`Queued session ${sessionId} for container device ${target.deviceId} and dispatched a boot; the queue drains when the worker connects`,
			);
			this.containerTargets?.boot(target.deviceId, { workspaceId, sessionId });
			return;
		}

		if (!this.notifiedSessions.has(sessionId)) {
			this.notifiedSessions.add(sessionId);
			await this.postActivity(
				workspaceId,
				sessionId,
				offlineWaitingMessage(target.email),
			);
			this.logger.info(
				`Device ${target.deviceId} offline; queued session ${sessionId} and posted waiting notice`,
			);
		}
	}

	private storedCreatorId(sessionId: string): string | undefined {
		return this.storedCreator(sessionId)?.id;
	}

	/**
	 * The full stored creator, not just the id: a lock rejection needs the NAME
	 * to say whose session is holding the issue, and it is the same stored blob.
	 */
	private storedCreator(sessionId: string): StoredCreator | undefined {
		const json = this.store.getSessionCreator(sessionId);
		if (!json) return undefined;
		try {
			return JSON.parse(json) as StoredCreator;
		} catch {
			this.logger.warn(
				`Corrupt stored creator for session ${sessionId}; skipping creator check`,
			);
			return undefined;
		}
	}

	/** Parses a queued payload and returns it only if it is an agent-session event. */
	private asSessionEvent(payloadJson: string): SessionEvent | undefined {
		let parsed: Webhook;
		try {
			parsed = JSON.parse(payloadJson) as Webhook;
		} catch {
			return undefined;
		}
		if (
			isAgentSessionCreatedWebhook(parsed) ||
			isAgentSessionPromptedWebhook(parsed)
		) {
			return parsed;
		}
		return undefined;
	}
}

/**
 * Same stop-shaped-body pattern `EdgeWorker.isTextStopRequest` uses on the
 * device side, so a held session recognizes a stop exactly like a running one
 * would.
 */
const STOP_BODY_RE = /^\s*stop(\s+session|\s+working)?[\s.!?]*$/i;

/**
 * Whether a `prompted` webhook is a stop request: either Linear's own `signal`
 * field, or a literal stop-shaped body (the same fallback the device applies —
 * see `EdgeWorker.handleUserPromptedAgentActivity`).
 */
function isStopRequest(webhook: SessionEvent): boolean {
	if (webhook.agentActivity?.signal === "stop") return true;
	const body = webhook.agentActivity?.content?.body;
	return typeof body === "string" && STOP_BODY_RE.test(body);
}

/**
 * Best-effort parent-issue id probe. The typed webhook issue payload
 * (`IssueWithDescriptionChildWebhookPayload`) does not expose a parent, so we
 * defensively read a `parentId` / `parent.id` if the runtime payload carries
 * one; otherwise this fallback is simply skipped.
 */
function extractParentIssueId(webhook: SessionEvent): string | undefined {
	const issue = webhook.agentSession.issue as unknown as
		| Record<string, unknown>
		| null
		| undefined;
	if (!issue) return undefined;

	const parentId = issue.parentId;
	if (typeof parentId === "string" && parentId.length > 0) {
		return parentId;
	}

	const parent = issue.parent;
	if (parent && typeof parent === "object") {
		const id = (parent as Record<string, unknown>).id;
		if (typeof id === "string" && id.length > 0) {
			return id;
		}
	}

	return undefined;
}

/**
 * Extracts the issue's human-readable key (e.g. "CYPACK-123") for routing a
 * container-executor user to their per-issue container. The typed webhook
 * issue payload exposes `identifier` directly, but this still reads it
 * defensively (like {@link extractParentIssueId}) rather than trusting the
 * compile-time type, since it flows into `ContainerTargetService.ensureDevice`
 * and from there into filesystem paths and Docker object names.
 */
function extractIssueKey(webhook: SessionEvent): string | undefined {
	const issue = webhook.agentSession.issue as unknown as
		| Record<string, unknown>
		| null
		| undefined;
	if (!issue) return undefined;
	const identifier = issue.identifier;
	return typeof identifier === "string" && identifier.length > 0
		? identifier
		: undefined;
}

/**
 * The device's own ISO timestamp, or `undefined` when it is absent or garbage.
 *
 * Never throws and never substitutes a router clock here — the caller decides
 * what to do with "the worker did not tell us", and silently swapping in the
 * router's `now()` inside a parser would make a worker-reported fact
 * indistinguishable from a router-invented one.
 */
function parseFrameTimestamp(value: string | undefined): number | undefined {
	if (value === undefined) return undefined;
	const ms = Date.parse(value);
	return Number.isFinite(ms) ? ms : undefined;
}

/**
 * Narrows an arbitrary worker-reported wait to the closed v1 vocabulary.
 *
 * `elicitation` is the only condition v1 models by name; everything else — a
 * reason from a newer worker, a typo, or a literal `other` — becomes `other`,
 * carrying text that says what the worker actually reported. The raw reason is
 * preserved as the condition rather than discarded, so narrowing loses the
 * classification but never the fact.
 *
 * The narrowing happens HERE, not in the frame schema, because the schema's only
 * way to reject a reason is a parse failure, and `DeviceGateway` answers that by
 * closing the device's socket — dropping every session on that worker over one
 * unmodelled string. See `sessionWait` in `cyrus-router-protocol`.
 *
 * A wait with no condition at all still gets one, because an `other` wait
 * without text records nothing an operator could act on — and "the worker
 * declined to say" is itself the actionable fact.
 */
function narrowWaitReason(
	wait: SessionStateFrame["wait"],
	nowMs: number,
): AgentRunWait {
	const sinceMs = parseFrameTimestamp(wait?.since) ?? nowMs;
	if (wait?.reason === WAIT_REASON_ELICITATION) {
		return { reason: "elicitation", sinceMs };
	}
	const reportedCondition =
		wait?.reportedCondition ??
		(wait?.reason !== undefined && wait.reason !== "other"
			? `unmodelled worker wait reason: ${wait.reason}`
			: "the worker reported `other` without describing the condition");
	return { reason: "other", sinceMs, reportedCondition };
}

/** The execution identity and pending-work count a worker reported, if any. */
function readFrameRunFacts(frame: SessionStateFrame): {
	runner?: string;
	model?: string;
	pendingWorkCount?: number;
} {
	return {
		...(frame.runner !== undefined ? { runner: frame.runner } : {}),
		...(frame.model !== undefined ? { model: frame.model } : {}),
		...(frame.pendingWorkCount !== undefined
			? { pendingWorkCount: frame.pendingWorkCount }
			: {}),
	};
}

/**
 * What the WEBHOOK alone can say about the Linear context an input arrived
 * under, for the run's routing snapshot.
 *
 * Read from the webhook rather than fetched, because this runs on the routing
 * path and a Linear round-trip here would delay every delivery. What the
 * webhook cannot answer is filled afterwards, off the path, by
 * `EventRouter.enrichRunRouting`.
 *
 * The split is not arbitrary — it is exactly what Linear puts on an
 * `AgentSessionEventWebhookPayload`. Its `agentSession.issue` is an
 * `IssueWithDescriptionChildWebhookPayload`, whose complete field set is
 * `{description?, id, identifier, team, teamId, title, url}`. So the TEAM is
 * here and free; the PROJECT is not on the payload at any nesting and costs a
 * fetch; and the workspace NAME appears nowhere on the webhook at all, only its
 * id. Reading through `unknown` rather than off the SDK type keeps this
 * tolerant of a payload that gains fields later.
 */
function extractRoutingSnapshot(webhook: SessionEvent): {
	workspaceId?: string;
	linearTeamId?: string;
	linearTeamName?: string;
} {
	const session = webhook.agentSession as unknown as Record<string, unknown>;
	const issue = session.issue as Record<string, unknown> | null | undefined;
	const team = issue?.team as Record<string, unknown> | null | undefined;

	const workspaceId = readNonEmptyString(webhook.organizationId);
	const teamId =
		readNonEmptyString(team?.id) ?? readNonEmptyString(issue?.teamId);
	const teamName = readNonEmptyString(team?.name);
	// A captured name never travels without its canonical id — that invariant is
	// enforced by `runRoutingSnapshotV1Schema`, and honouring it here keeps a
	// name-only payload from producing an unemittable observation later.
	return {
		...(workspaceId ? { workspaceId } : {}),
		...(teamId ? { linearTeamId: teamId } : {}),
		...(teamId && teamName ? { linearTeamName: teamName } : {}),
	};
}

/** Stable Linear references for the input that caused this routing decision. */
function extractRunInput(
	webhook: SessionEvent,
	routedMs: number,
): { activityId?: string; commentId?: string; routedMs: number } {
	const activity = webhook.agentActivity as unknown as
		| Record<string, unknown>
		| null
		| undefined;
	const session = webhook.agentSession as unknown as Record<string, unknown>;
	const sessionComment = session.comment as Record<string, unknown> | undefined;
	const activityId = readNonEmptyString(activity?.id);
	const commentId = isAgentSessionPromptedWebhook(webhook)
		? readNonEmptyString(activity?.sourceCommentId)
		: (readNonEmptyString(session.sourceCommentId) ??
			readNonEmptyString(session.commentId) ??
			readNonEmptyString(sessionComment?.id));
	return {
		...(activityId ? { activityId } : {}),
		...(commentId ? { commentId } : {}),
		routedMs,
	};
}

function readNonEmptyString(value: unknown): string | undefined {
	return typeof value === "string" && value.length > 0 ? value : undefined;
}
