import { existsSync, mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	type AgentEvent,
	CANONICAL_RUN_ATTRIBUTE_KEYS,
	type ILogger,
} from "cyrus-core";
import type {
	ContainerExecutor,
	IssueExecutionContext,
} from "cyrus-router-executors";
import { beforeEach, describe, expect, it, type Mock, vi } from "vitest";
import { ContainerTargetService } from "../src/ContainerTargets.js";
import {
	EventRouter,
	type EventRouterOptions,
	TERMINAL_OWNERSHIP_GRACE_MS,
} from "../src/EventRouter.js";
import {
	expiredMessage,
	fillTemplate,
	INVALID_ISSUE_KEY_MESSAGE,
	ISSUE_LOCKED_MESSAGE,
	ORPHANED_LOCK_RECLAIMED_MESSAGE,
	offlineReleaseMessage,
	offlineWaitingMessage,
	PROMPT_REJECTION_MESSAGE,
	PROMPT_UNROUTABLE_MESSAGE,
	UNENROLLED_CREATOR_MESSAGE,
} from "../src/messages.js";
import { RouterStore } from "../src/RouterStore.js";
import { SecretStore } from "../src/SecretStore.js";
import { TerminalTeardown } from "../src/TerminalTeardown.js";
import {
	eventsNamed,
	silentLogger,
	type TestLogger,
	testLogger,
} from "./helpers/logger.js";

const ROUTE_NOW = 1_000_000;
const TTL_MS = 60_000;

/**
 * The canonical attribution bag (CYR-72) as `eventsNamed` renders it — i.e.
 * with the `cyrus.` prefix stripped — when nothing is known.
 *
 * Every router event carries all sixteen keys, `null` for the facts that were
 * not known at emission time, so `where isnull(p["cyrus.run_id"])` finds the
 * lines a run never produced. Tests spread this and then state only the facts
 * their own scenario supplies.
 */
const NO_RUN_ATTRIBUTION = {
	workspace_id: null,
	workspace_name: null,
	owner_id: null,
	owner_name: null,
	team_id: null,
	team_name: null,
	project_id: null,
	project_name: null,
	issue_key: null,
	run_id: null,
	session_id: null,
	device_id: null,
	runner: null,
	model: null,
	provider: null,
	source: "router",
};

interface Creator {
	id: string;
	email: string;
	name: string;
}

/** Minimal object that satisfies isAgentSessionCreatedWebhook + fields we read. */
function createdEvent(opts: {
	sessionId: string;
	issueId?: string;
	/** The issue's human-readable key, e.g. "CYPACK-1" (drives container issueKey resolution). */
	identifier?: string;
	creator?: Creator;
	organizationId?: string;
	parentIssueId?: string;
	commentId?: string;
}): AgentEvent {
	const org = opts.organizationId ?? "ws-1";
	return {
		type: "AgentSessionEvent",
		action: "created",
		organizationId: org,
		agentSession: {
			id: opts.sessionId,
			organizationId: org,
			issueId: opts.issueId,
			issue: opts.issueId
				? {
						id: opts.issueId,
						identifier: opts.identifier,
						parentId: opts.parentIssueId,
					}
				: undefined,
			creator: opts.creator,
			commentId: opts.commentId,
		},
	} as unknown as AgentEvent;
}

/** Minimal fake ContainerExecutor whose ensureRunning is an inspectable mock. */
function fakeExecutor(
	provider: string,
	overrides?: { ensureRunning?: Mock },
): ContainerExecutor & { ensureRunning: Mock } {
	return {
		provider,
		ensureRunning:
			overrides?.ensureRunning ??
			vi.fn<(ctx: IssueExecutionContext) => Promise<void>>(async () => {}),
		destroy: vi.fn(async () => {}),
		stop: vi.fn(async () => {}),
		status: vi.fn(async () => "running" as const),
		listManaged: vi.fn(async () => []),
	};
}

/** A real ContainerTargetService over the same store, backed by a fake executor. */
function makeContainerTargets(
	store: RouterStore,
	overrides?: { executor?: ReturnType<typeof fakeExecutor> },
) {
	const secrets = new SecretStore(
		join(mkdtempSync(join(tmpdir(), "event-router-secrets-")), "secrets.json"),
	);
	const postActivity = vi.fn<
		(workspaceId: string, agentSessionId: string, body: string) => Promise<void>
	>(async () => {});
	const executor = overrides?.executor ?? fakeExecutor("docker");
	const containerTargets = new ContainerTargetService({
		store,
		secrets,
		executors: new Map([["docker", executor]]),
		// A single default repo, not empty: this file's boot-success tests
		// assert `executor.ensureRunning` is actually called, which
		// `ContainerTargets.buildEnv` never reaches when nothing in the
		// registry resolves (it throws before calling the executor).
		registry: {
			list: vi.fn(async () => ({
				repositories: [
					{
						name: "cyrus",
						githubSlug: "ceedaragents/cyrus",
						linearWorkspaceId: "ws-1",
						isDefault: true,
					},
				],
			})),
			put: vi.fn(async () => ({ version: "1" })),
		},
		containersConfig: {
			routerUrlForContainers: "wss://router.example.com",
		},
		postActivity,
		logger: silentLogger(),
	});
	return { containerTargets, executor, secrets, postActivity };
}

/** Minimal object that satisfies isAgentSessionPromptedWebhook + fields we read. */
function promptedEvent(opts: {
	sessionId: string;
	actorUserId?: string;
	creator?: Creator;
	issueId?: string;
	/**
	 * The issue's human-readable key, e.g. "CYPACK-1" (drives container
	 * issueKey resolution). Omit to model a webhook whose `agentSession.issue`
	 * carries no `identifier` at all (`issue` itself is `Maybe<...>` on the
	 * real Linear payload type, so this is a real, not merely theoretical,
	 * shape) — see the "fails closed" tests below.
	 */
	identifier?: string;
	organizationId?: string;
	commentId?: string;
}): AgentEvent {
	const org = opts.organizationId ?? "ws-1";
	return {
		type: "AgentSessionEvent",
		action: "prompted",
		organizationId: org,
		agentActivity: opts.actorUserId
			? {
					id: "act-1",
					userId: opts.actorUserId,
					sourceCommentId: opts.commentId,
					content: {},
				}
			: undefined,
		agentSession: {
			id: opts.sessionId,
			organizationId: org,
			issueId: opts.issueId,
			issue: opts.issueId
				? { id: opts.issueId, identifier: opts.identifier }
				: undefined,
			creator: opts.creator,
		},
	} as unknown as AgentEvent;
}

/**
 * Minimal object that satisfies isIssueStateChangeWebhook. Linear sends this
 * (as an AppUserNotification) when an issue reaches a terminal state; the node
 * turns it into the IssueStateChangeMessage that drives worktree cleanup.
 */
function issueStatusChangedEvent(opts: {
	issueId: string;
	identifier?: string;
	organizationId?: string;
	/**
	 * The payload's own `createdAt`, which is part of the webhook idempotency
	 * key. Two genuinely separate terminal notifications for one issue (Done →
	 * reopen → Done) carry different payload timestamps; reusing one timestamp
	 * models a REDELIVERY of a single notification, which `route()` now drops.
	 */
	createdAtMs?: number;
}): AgentEvent {
	return {
		type: "AppUserNotification",
		action: "issueStatusChanged",
		organizationId: opts.organizationId ?? "ws-1",
		createdAt: new Date(opts.createdAtMs ?? ROUTE_NOW).toISOString(),
		notification: {
			issue: {
				id: opts.issueId,
				identifier: opts.identifier ?? "TEST-1",
			},
		},
	} as unknown as AgentEvent;
}

/** A regular Linear comment, which has no agent-session envelope of its own. */
function issueNewCommentEvent(opts: {
	issueId: string;
	identifier?: string;
	organizationId?: string;
	createdAtMs?: number;
}): AgentEvent {
	return {
		type: "AppUserNotification",
		action: "issueNewComment",
		organizationId: opts.organizationId ?? "ws-1",
		createdAt: new Date(opts.createdAtMs ?? ROUTE_NOW).toISOString(),
		notification: {
			issue: {
				id: opts.issueId,
				identifier: opts.identifier ?? "TEST-1",
			},
			comment: {
				id: "comment-1",
				body: "Please continue",
				userId: "lin-alice",
			},
			actorId: "lin-alice",
		},
	} as unknown as AgentEvent;
}

/** Minimal object that satisfies isIssueDeletedWebhook (a deleted issue is terminal too). */
function issueDeletedEvent(opts: {
	issueId: string;
	identifier?: string;
	organizationId?: string;
}): AgentEvent {
	return {
		type: "Issue",
		action: "remove",
		organizationId: opts.organizationId ?? "ws-1",
		createdAt: new Date(ROUTE_NOW).toISOString(),
		data: {
			id: opts.issueId,
			identifier: opts.identifier ?? "TEST-1",
		},
	} as unknown as AgentEvent;
}

function enroll(
	store: RouterStore,
	email: string,
	opts?: { name?: string; linearId?: string },
): number {
	store.addUser({ email, name: opts?.name, linearId: opts?.linearId });
	const code = store.mintEnrollmentCode(email, 1);
	const device = store.redeemEnrollmentCode(code, 1);
	if (!device) throw new Error("enroll failed");
	return device.deviceId;
}

interface Gateway {
	isOnline: () => boolean;
	deliverPending: Mock<(deviceId: number) => void>;
}

function makeRouter(
	store: RouterStore,
	overrides?: {
		gateway?: Gateway;
		containerTargets?: ContainerTargetService;
		terminalTeardown?: TerminalTeardown;
		logger?: ILogger;
		fetchRoutingContext?: EventRouterOptions["fetchRoutingContext"];
		config?: Partial<{
			eventTtlMs: number;
			issueLock: boolean;
			creatorOnlyPrompting: boolean;
			affinityGraceMs: number;
		}>;
	},
) {
	const postActivity = vi.fn<
		(workspaceId: string, agentSessionId: string, body: string) => Promise<void>
	>(async () => {});
	const moveIssueToStartedState = vi.fn<
		(workspaceId: string, issueId: string) => Promise<string | undefined>
	>(async () => "In Progress");
	const clock = { value: ROUTE_NOW };
	const gateway: Gateway = overrides?.gateway ?? {
		isOnline: () => false,
		deliverPending: vi.fn<(deviceId: number) => void>(),
	};
	const router = new EventRouter({
		store,
		gateway,
		postActivity,
		moveIssueToStartedState,
		...(overrides?.fetchRoutingContext
			? { fetchRoutingContext: overrides.fetchRoutingContext }
			: {}),
		containerTargets: overrides?.containerTargets,
		terminalTeardown: overrides?.terminalTeardown,
		config: {
			eventTtlMs: TTL_MS,
			issueLock: true,
			creatorOnlyPrompting: false,
			affinityGraceMs: 600_000,
			...overrides?.config,
		},
		logger: overrides?.logger ?? silentLogger(),
		now: () => clock.value,
	});
	return { router, postActivity, moveIssueToStartedState, gateway, clock };
}

const ALICE: Creator = {
	id: "lin-alice",
	email: "alice@example.com",
	name: "Alice",
};
const BOB: Creator = { id: "lin-bob", email: "bob@example.com", name: "Bob" };

describe("EventRouter", () => {
	let store: RouterStore;

	beforeEach(() => {
		store = new RouterStore(":memory:");
	});

	it("(a) routes a created event by creator email, queues it, and posts the offline notice once per session", async () => {
		// No linearId → creator.id won't match, forcing email-based routing.
		const deviceId = enroll(store, "alice@example.com", { name: "Alice" });
		const { router, postActivity } = makeRouter(store);
		const creator: Creator = {
			id: "lin-unmatched",
			email: "alice@example.com",
			name: "Alice",
		};

		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator }),
		);
		expect(store.pendingEvents(deviceId, 0, ROUTE_NOW)).toHaveLength(1);

		// A second event on the same session while still offline: queued again,
		// but the offline notice must NOT be posted a second time.
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator }),
		);
		expect(store.pendingEvents(deviceId, 0, ROUTE_NOW)).toHaveLength(2);

		const waiting = postActivity.mock.calls.filter(
			(c) => c[2] === offlineWaitingMessage("alice@example.com"),
		);
		expect(waiting).toHaveLength(1);
		expect(waiting[0]).toEqual([
			"ws-1",
			"sess-1",
			offlineWaitingMessage("alice@example.com"),
		]);
	});

	it("forwards an ordinary issue comment to the issue's active device", async () => {
		const deviceId = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router } = makeRouter(store);
		store.setIssueAffinity("ISS-1", deviceId);

		const comment = issueNewCommentEvent({ issueId: "ISS-1" });
		await router.route(comment);
		await router.route(comment);

		const [queued] = store.pendingEvents(deviceId, 0, ROUTE_NOW);
		expect(queued).toBeDefined();
		expect(store.pendingEvents(deviceId, 0, ROUTE_NOW)).toHaveLength(1);
		expect(JSON.parse(queued!.payloadJson)).toMatchObject({
			type: "AppUserNotification",
			action: "issueNewComment",
		});
	});

	it("records routed input ids and the exact terminal run outcome", async () => {
		const deviceId = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router, clock } = makeRouter(store);

		await router.route(
			createdEvent({
				sessionId: "sess-observed",
				issueId: "issue-observed",
				identifier: "NOR-402",
				creator: ALICE,
				commentId: "comment-created",
			}),
		);
		clock.value += 100;
		await router.route(
			promptedEvent({
				sessionId: "sess-observed",
				issueId: "issue-observed",
				identifier: "NOR-402",
				creator: ALICE,
				actorUserId: "lin-alice",
				commentId: "comment-prompted",
			}),
		);
		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "terminal-1",
			sessionId: "sess-observed",
			state: "complete",
		});

		const [run] = store.listAgentRuns({ userId: 1 });
		expect(run).toMatchObject({
			issueKey: "NOR-402",
			sessionId: "sess-observed",
			state: "complete",
			endedMs: ROUTE_NOW + 100,
		});
		expect(run?.inputs).toEqual([
			{ commentId: "comment-created", routedMs: ROUTE_NOW },
			{
				activityId: "act-1",
				commentId: "comment-prompted",
				routedMs: ROUTE_NOW + 100,
			},
		]);
	});

	it("(b) posts the unenrolled-creator message and queues nothing for an unknown creator", async () => {
		const { router, postActivity } = makeRouter(store);
		const creator: Creator = {
			id: "lin-charlie",
			email: "charlie@example.com",
			name: "Charlie",
		};

		await router.route(
			createdEvent({ sessionId: "sess-x", issueId: "ISS-9", creator }),
		);

		expect(postActivity).toHaveBeenCalledTimes(1);
		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-x",
			fillTemplate(UNENROLLED_CREATOR_MESSAGE, { userName: "Charlie" }),
		);
		// Nothing recorded for a creator we can't route.
		expect(store.getSessionAffinity("sess-x")).toBeUndefined();
		expect(store.getIssueLock("ISS-9")).toBeUndefined();
	});

	it("(c) rejects a second created event on a locked issue from a different session", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const bobDevice = enroll(store, "bob@example.com", { linearId: "lin-bob" });
		const { router, postActivity } = makeRouter(store);

		await router.route(
			createdEvent({ sessionId: "sess-a", issueId: "ISS-1", creator: ALICE }),
		);
		postActivity.mockClear();

		await router.route(
			createdEvent({ sessionId: "sess-b", issueId: "ISS-1", creator: BOB }),
		);

		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-b",
			ISSUE_LOCKED_MESSAGE,
		);
		// Bob's device must not have received the event.
		expect(store.pendingEvents(bobDevice, 0, ROUTE_NOW)).toHaveLength(0);
		// The lock still belongs to Alice's session.
		expect(store.getIssueLock("ISS-1")).toEqual({
			sessionId: "sess-a",
			deviceId: aliceDevice,
		});
	});

	it("(c2) makes a lock rejection observable, naming the session that holds the issue", async () => {
		// NOR-402's third acceptance criterion. A rejection here leaves the ISSUE
		// unreachable — the reply goes only into the rejected session's own
		// thread, which nobody reads — and it used to be an `info` line among 220
		// near-identical ones, indistinguishable from a webhook that never came.
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		enroll(store, "bob@example.com", { linearId: "lin-bob" });
		const logger = testLogger();
		const { router } = makeRouter(store, { logger });

		await router.route(
			createdEvent({ sessionId: "sess-a", issueId: "ISS-2", creator: ALICE }),
		);
		await router.route(
			createdEvent({ sessionId: "sess-b", issueId: "ISS-2", creator: BOB }),
		);

		expect(eventsNamed(logger, "routing.rejected")).toEqual([
			{
				...NO_RUN_ATTRIBUTION,
				// From the webhook's own routing snapshot: a rejection has no run, so
				// this is the only workspace the router can honestly attribute it to.
				workspace_id: "ws-1",
				session_id: "sess-b",
				reason: "issue_locked",
				agent_session_id: "sess-b",
				issue_id: "ISS-2",
				held_by_session_id: "sess-a",
				held_by_device_id: aliceDevice,
			},
		]);
		// WARN, so it survives a sink whose threshold is WARN+ and stands out on a
		// console full of routine routing lines.
		expect(
			logger.warn.mock.calls.filter(([m]) =>
				String(m).includes("The prompt did NOT reach an agent"),
			),
		).toHaveLength(1);
	});

	it("(c3) makes an unenrolled-creator refusal observable too", async () => {
		const logger = testLogger();
		const { router } = makeRouter(store, { logger });

		await router.route(
			createdEvent({
				sessionId: "sess-u",
				issueId: "ISS-3",
				creator: { name: "Charlie", email: "charlie@example.com" },
			}),
		);

		expect(eventsNamed(logger, "routing.rejected")).toEqual([
			{
				...NO_RUN_ATTRIBUTION,
				workspace_id: "ws-1",
				session_id: "sess-u",
				reason: "unenrolled_creator",
				agent_session_id: "sess-u",
				issue_id: "ISS-3",
				held_by_session_id: null,
				held_by_device_id: null,
				// owner_* stays null on purpose: an unenrolled creator is precisely
				// the case where there is no Cyrus user to attribute the run to, and
				// naming the webhook's actor would put a non-existent owner into the
				// column every ownership filter reads.
			},
		]);
	});

	it("(c4) makes a non-creator PROMPT refusal observable — the path the lock message steers users into", async () => {
		// The gap that made AC3 only half true. `ISSUE_LOCKED_MESSAGE` answers a
		// lock rejection by telling the user to reply inside the holding session's
		// thread, i.e. it routes every lock-rejected user out of `routeCreated`
		// and into `routePrompted` — whose refusals emitted nothing and logged at
		// `info`. A user following the product's own advice and being refused
		// there reproduced NOR-402 exactly: a comment that reached no agent, with
		// nothing in the log stream saying so.
		enroll(store, "alice@example.com", { linearId: "lin-alice" });
		const logger = testLogger();
		const { router } = makeRouter(store, {
			logger,
			config: { creatorOnlyPrompting: true },
		});
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);

		await router.route(
			promptedEvent({
				sessionId: "sess-1",
				issueId: "ISS-1",
				actorUserId: "lin-bob",
				creator: ALICE,
			}),
		);

		// This session WAS routed, so unlike (c2)/(c3) the refusal carries the
		// canonical facts of the run it was refused against — the property CYR-72
		// exists for: one `where p["cyrus.run_id"] == …` returns the route, the
		// refusal, and everything the worker logged in between.
		expect(eventsNamed(logger, "routing.rejected")).toContainEqual({
			...NO_RUN_ATTRIBUTION,
			workspace_id: "ws-1",
			owner_id: "1",
			owner_name: "alice@example.com",
			session_id: "sess-1",
			device_id: 1,
			run_id: expect.any(String),
			// The run row stores the literal "unknown" for a webhook that carried
			// no issue key; it is normalised back to null on the way out, so
			// `isnull(...)` finds these rather than a sentinel gathering every
			// unrelated key-less run under one plausible-looking issue.
			issue_key: null,
			reason: "non_creator_prompt",
			agent_session_id: "sess-1",
			issue_id: "ISS-1",
			held_by_session_id: null,
			held_by_device_id: null,
		});
	});

	it("(c5) tells a different user that the holder is someone else's session, instead of looping them", async () => {
		// With `creatorOnlyPrompting` on (the default), "reply in the running
		// session's thread" is FALSE advice for a non-creator: that reply is
		// rejected and tells them to start their own session, which lands back on
		// the lock. The old single message dropped the one fact that explains the
		// loop.
		enroll(store, "alice@example.com", { linearId: "lin-alice" });
		enroll(store, "bob@example.com", { linearId: "lin-bob" });
		const { router, postActivity } = makeRouter(store, {
			config: { creatorOnlyPrompting: true },
		});

		await router.route(
			createdEvent({ sessionId: "sess-a", issueId: "ISS-9", creator: ALICE }),
		);
		postActivity.mockClear();
		await router.route(
			createdEvent({ sessionId: "sess-b", issueId: "ISS-9", creator: BOB }),
		);

		const [, , body] = postActivity.mock.calls[0] as [string, string, string];
		expect(body).toContain("Alice");
		expect(body).not.toContain("Reply inside the running session's thread");
		// Never hand the destructive remedy to a Linear reader: the router itself
		// cannot tell a strand from a session waiting on a scheduled wakeup.
		expect(body).not.toContain("cyrus router unlock");
	});

	it("(c6) keeps the same-user lock message, which is the verified recovery", async () => {
		// CAN-133 was a self-collision. Telling that user the holder belongs to
		// "another user" sent them looking for a colleague who did not exist.
		enroll(store, "alice@example.com", { linearId: "lin-alice" });
		const { router, postActivity } = makeRouter(store, {
			config: { creatorOnlyPrompting: true },
		});

		await router.route(
			createdEvent({ sessionId: "sess-a", issueId: "ISS-8", creator: ALICE }),
		);
		postActivity.mockClear();
		await router.route(
			createdEvent({ sessionId: "sess-b", issueId: "ISS-8", creator: ALICE }),
		);

		const [, , body] = postActivity.mock.calls[0] as [string, string, string];
		expect(body).toContain("Reply inside the running session's thread");
		expect(body).not.toContain("cyrus router unlock");
	});

	it("(d) enforces creator-only prompting using the activity actor field", async () => {
		// creatorOnlyPrompting: true → a prompt from a non-creator actor is rejected.
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router, postActivity } = makeRouter(store, {
			config: { creatorOnlyPrompting: true },
		});
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);
		postActivity.mockClear();
		const queuedBefore = store.pendingEvents(aliceDevice, 0, ROUTE_NOW).length;

		await router.route(
			promptedEvent({
				sessionId: "sess-1",
				actorUserId: "lin-bob",
				creator: ALICE,
			}),
		);

		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			PROMPT_REJECTION_MESSAGE,
		);
		expect(store.pendingEvents(aliceDevice, 0, ROUTE_NOW)).toHaveLength(
			queuedBefore,
		);

		// creatorOnlyPrompting: false → the same non-creator prompt IS routed.
		const store2 = new RouterStore(":memory:");
		const aliceDevice2 = enroll(store2, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router: router2 } = makeRouter(store2, {
			config: { creatorOnlyPrompting: false },
		});
		await router2.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);
		const before2 = store2.pendingEvents(aliceDevice2, 0, ROUTE_NOW).length;
		await router2.route(
			promptedEvent({
				sessionId: "sess-1",
				actorUserId: "lin-bob",
				creator: ALICE,
			}),
		);
		expect(store2.pendingEvents(aliceDevice2, 0, ROUTE_NOW).length).toBe(
			before2 + 1,
		);
	});

	it("(d2) rejects a prompt whose actor cannot be identified (fails closed, not open)", async () => {
		// Regression test: a real non-creator webhook may omit `agentActivity.userId`.
		// `agentSession.creator` is ALWAYS the session's original creator (Alice)
		// regardless of who is actually prompting, so falling back to it would
		// make the actor look identical to the creator and let a stranger's
		// prompt through. The gate must fail CLOSED when the actor is unknown.
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router, postActivity } = makeRouter(store, {
			config: { creatorOnlyPrompting: true },
		});
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);
		postActivity.mockClear();
		const queuedBefore = store.pendingEvents(aliceDevice, 0, ROUTE_NOW).length;

		await router.route(
			promptedEvent({
				sessionId: "sess-1",
				// No actorUserId → agentActivity is omitted entirely, so
				// agentActivity?.userId is undefined. agentSession.creator still
				// reports Alice (the true session creator), as it always does.
				creator: ALICE,
			}),
		);

		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			PROMPT_REJECTION_MESSAGE,
		);
		expect(store.pendingEvents(aliceDevice, 0, ROUTE_NOW)).toHaveLength(
			queuedBefore,
		);
	});

	it("(d3) routes a prompt whose session affinity was released by a terminal state", async () => {
		// Regression: a Linear agent session outlives its turns — the user can
		// prompt it again after it completes. The terminal state releases session
		// affinity, and routePrompted used to resolve on affinity ALONE, so every
		// follow-up prompt was dropped silently and the session sat in "Waiting
		// for Cyrus" forever. A new session was the only way out.
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router, postActivity } = makeRouter(store);
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);

		// The session finishes: the router releases its lock AND its affinity.
		router.handleSessionState(aliceDevice, {
			sessionId: "sess-1",
			state: "complete",
		} as any);
		expect(store.getSessionAffinity("sess-1")).toBeUndefined();

		postActivity.mockClear();
		const queuedBefore = store.pendingEvents(aliceDevice, 0, ROUTE_NOW).length;

		await router.route(
			promptedEvent({
				sessionId: "sess-1",
				issueId: "ISS-1",
				actorUserId: "lin-alice",
				creator: ALICE,
			}),
		);

		// Falls back to the creator's enrolled device rather than dropping.
		expect(store.pendingEvents(aliceDevice, 0, ROUTE_NOW).length).toBe(
			queuedBefore + 1,
		);
		expect(postActivity).not.toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			PROMPT_UNROUTABLE_MESSAGE,
		);
		// ...and affinity is re-established, so the next prompt takes the fast path
		// and the creator-only gate has a stored creator to compare against again.
		expect(store.getSessionAffinity("sess-1")).toBe(aliceDevice);
	});

	it("(d3b) still enforces creator-only prompting on a session rescued by the fallback", async () => {
		// The fallback in (d3) must not become a way around the creator gate. A
		// rescued session has no STORED creator (the terminal state deleted the
		// affinity row), so the gate has to fall back to the session creator the
		// webhook carries — otherwise Bob could prompt Alice's finished session
		// onto Alice's machine.
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router, postActivity } = makeRouter(store, {
			config: { creatorOnlyPrompting: true },
		});
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);
		router.handleSessionState(aliceDevice, {
			sessionId: "sess-1",
			state: "complete",
		} as any);
		expect(store.getSessionAffinity("sess-1")).toBeUndefined();

		postActivity.mockClear();
		const queuedBefore = store.pendingEvents(aliceDevice, 0, ROUTE_NOW).length;

		await router.route(
			promptedEvent({
				sessionId: "sess-1",
				issueId: "ISS-1",
				actorUserId: "lin-bob",
				creator: ALICE,
			}),
		);

		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			PROMPT_REJECTION_MESSAGE,
		);
		expect(store.pendingEvents(aliceDevice, 0, ROUTE_NOW)).toHaveLength(
			queuedBefore,
		);
		// Rejected prompts must not resurrect affinity either.
		expect(store.getSessionAffinity("sess-1")).toBeUndefined();
	});

	it("(d4) tells the user when a prompt cannot be routed instead of dropping it silently", async () => {
		// Nothing resolves: no session affinity, no enrolled device for the
		// creator, no issue affinity. The session must not be left waiting.
		const { router, postActivity } = makeRouter(store);

		await router.route(
			promptedEvent({
				sessionId: "sess-unknown",
				issueId: "ISS-unknown",
				actorUserId: "lin-bob",
				creator: BOB,
			}),
		);

		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-unknown",
			PROMPT_UNROUTABLE_MESSAGE,
		);
	});

	it("(e) handleSessionState(complete) releases the lock so a new session can acquire it", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router } = makeRouter(store);
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);
		// Issue is locked by sess-1: a different session cannot acquire it.
		expect(store.acquireIssueLock("ISS-1", "sess-2", aliceDevice)).toBe(false);

		router.handleSessionState(aliceDevice, {
			type: "session_state",
			id: "ss-1",
			sessionId: "sess-1",
			state: "complete",
		});

		expect(store.getSessionAffinity("sess-1")).toBeUndefined();
		expect(store.acquireIssueLock("ISS-1", "sess-2", aliceDevice)).toBe(true);
	});

	// ── parked (non-terminal) session state ────────────────────────────────
	// A session blocked on a user answer with no work in flight releases its
	// device so ContainerLifecycle can idle-suspend the container — but it is
	// NOT finished, so it keeps the issue lock.

	it("(e2) handleSessionState(parked) releases affinity but keeps the issue lock", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router } = makeRouter(store);
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);

		router.handleSessionState(aliceDevice, {
			type: "session_state",
			id: "ss-1",
			sessionId: "sess-1",
			state: "parked",
		});

		// Affinity gone → the sweep's affinity gate no longer protects the device.
		expect(store.getSessionAffinity("sess-1")).toBeUndefined();
		// Lock retained → no other session can claim the issue mid-conversation.
		expect(store.acquireIssueLock("ISS-1", "sess-2", aliceDevice)).toBe(false);
	});

	it("(e3) handleSessionState(parked) stamps parkedAtMs on the device", async () => {
		const { userId } = store.addUser({ email: "alice@example.com" });
		const { deviceId } = store.createContainerDevice(userId, "PAR-146", "aca");
		const { router, clock } = makeRouter(store);
		store.setSessionAffinity("sess-1", deviceId);
		clock.value = 5_000;

		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "ss-1",
			sessionId: "sess-1",
			state: "parked",
		});

		expect(store.getContainerDeviceForIssue("PAR-146")?.parkedAtMs).toBe(5_000);
	});

	it("(e3a) emits sandbox park/unpark events for a container device", async () => {
		const { userId } = store.addUser({ email: "alice@example.com" });
		const { deviceId } = store.createContainerDevice(userId, "PAR-146", "aca");
		const logger = testLogger();
		const { router } = makeRouter(store, { logger });
		store.setSessionAffinity("sess-1", deviceId);

		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "ss-1",
			sessionId: "sess-1",
			state: "parked",
		});
		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "ss-2",
			sessionId: "sess-1",
			state: "active",
		});

		expect(logger.event).toHaveBeenCalledWith("sandbox.parked", {
			"cyrus.issue_key": "PAR-146",
			"cyrus.device_id": deviceId,
			"cyrus.provider": "aca",
			"cyrus.session_id": "sess-1",
		});
		expect(logger.event).toHaveBeenCalledWith("sandbox.unparked", {
			"cyrus.issue_key": "PAR-146",
			"cyrus.device_id": deviceId,
			"cyrus.provider": "aca",
			"cyrus.session_id": "sess-1",
		});
	});

	/**
	 * `handleSessionState` is shared by physical devices and sandboxes, but the
	 * `sandbox.*` family has to stay countable as sandboxes — a teammate's laptop
	 * parking a session is not a fleet-cost signal and must not appear in it.
	 */
	it("(e3b) emits no sandbox event when the parking device is a physical laptop", async () => {
		const deviceId = enroll(store, "alice@example.com");
		const logger = testLogger();
		const { router } = makeRouter(store, { logger });
		store.setSessionAffinity("sess-1", deviceId);

		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "ss-1",
			sessionId: "sess-1",
			state: "parked",
		});

		expect(
			logger.event.mock.calls.filter(([name]) =>
				String(name).startsWith("sandbox."),
			),
		).toHaveLength(0);
	});

	it("(e4) a prompt after parking re-establishes affinity and clears the stamp", async () => {
		const { userId } = store.addUser({ email: "alice@example.com" });
		const { deviceId } = store.createContainerDevice(userId, "PAR-146", "aca");
		const { router } = makeRouter(store);
		store.setSessionAffinity("sess-1", deviceId);
		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "ss-1",
			sessionId: "sess-1",
			state: "parked",
		});
		expect(
			store.getContainerDeviceForIssue("PAR-146")?.parkedAtMs,
		).toBeDefined();

		// The user's answer routes back to the same device.
		store.setSessionAffinity("sess-1", deviceId);

		expect(
			store.getContainerDeviceForIssue("PAR-146")?.parkedAtMs,
		).toBeUndefined();
	});

	// ── active (unpark) session state ──────────────────────────────────────
	// A park that is never answered — the elicitation was abandoned, or the
	// agent went back to work — must be revocable by the device itself.
	// Without this the session runs on with no affinity, so every
	// session-scoped RPC it makes is rejected with "session not owned by this
	// device" and its whole turn is silently dropped (PAR-146).

	it("(e5) handleSessionState(active) restores affinity and clears the stamp", () => {
		const { userId } = store.addUser({ email: "alice@example.com" });
		const { deviceId } = store.createContainerDevice(userId, "PAR-146", "aca");
		const { router } = makeRouter(store);
		store.setSessionAffinity("sess-1", deviceId);
		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "ss-1",
			sessionId: "sess-1",
			state: "parked",
		});

		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "ss-2",
			sessionId: "sess-1",
			state: "active",
		});

		expect(store.getSessionAffinity("sess-1")).toBe(deviceId);
		expect(
			store.getContainerDeviceForIssue("PAR-146")?.parkedAtMs,
		).toBeUndefined();
	});

	it("(e6) handleSessionState(active) preserves the session creator", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router } = makeRouter(store);
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);
		const creatorBefore = store.getSessionCreator("sess-1");
		expect(creatorBefore).toBeDefined();

		router.handleSessionState(aliceDevice, {
			type: "session_state",
			id: "ss-1",
			sessionId: "sess-1",
			state: "parked",
		});
		router.handleSessionState(aliceDevice, {
			type: "session_state",
			id: "ss-2",
			sessionId: "sess-1",
			state: "active",
		});

		// `creator_json` gates who may prompt a session. Restoring affinity with
		// a null creator would drop that record on the floor.
		expect(store.getSessionCreator("sess-1")).toBe(creatorBefore);
	});

	it("(e7) handleSessionState(active) keeps the issue lock held", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router } = makeRouter(store);
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);

		router.handleSessionState(aliceDevice, {
			type: "session_state",
			id: "ss-1",
			sessionId: "sess-1",
			state: "parked",
		});
		router.handleSessionState(aliceDevice, {
			type: "session_state",
			id: "ss-2",
			sessionId: "sess-1",
			state: "active",
		});

		// Unparking is a resumption, not a completion.
		expect(store.acquireIssueLock("ISS-1", "sess-2", aliceDevice)).toBe(false);
	});

	it("(e8) handleSessionState(terminal) grants a posting grace to the reporting device", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router, clock } = makeRouter(store);
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);

		router.handleSessionState(aliceDevice, {
			type: "session_state",
			id: "ss-1",
			sessionId: "sess-1",
			state: "complete",
		});

		// The lock and affinity are both gone, so the grace is the ONLY thing
		// still authorizing the closing summary the worker is mid-flight with.
		expect(store.getSessionAffinity("sess-1")).toBeUndefined();
		expect(store.getIssueLockDeviceForSession("sess-1")).toBeUndefined();
		expect(store.getSessionOwnershipGrace("sess-1", clock.value)).toBe(
			aliceDevice,
		);
		expect(
			store.getSessionOwnershipGrace(
				"sess-1",
				clock.value + TERMINAL_OWNERSHIP_GRACE_MS,
			),
		).toBeUndefined();
	});

	it("(e9) a replayed active after a terminal state cannot resurrect affinity", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router } = makeRouter(store);
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);
		router.handleSessionState(aliceDevice, {
			type: "session_state",
			id: "ss-1",
			sessionId: "sess-1",
			state: "parked",
		});
		router.handleSessionState(aliceDevice, {
			type: "session_state",
			id: "ss-2",
			sessionId: "sess-1",
			state: "complete",
		});

		// At-least-once delivery means an unacked `active` can arrive after the
		// session has already finished. Re-pinning a finished session to a device
		// would block the sweep from ever reclaiming that container.
		router.handleSessionState(aliceDevice, {
			type: "session_state",
			id: "ss-3",
			sessionId: "sess-1",
			state: "active",
		});

		expect(store.getSessionAffinity("sess-1")).toBeUndefined();
	});

	it("(e8) handleSessionState(active) is inert for a session that never parked", () => {
		const { userId } = store.addUser({ email: "alice@example.com" });
		const { deviceId } = store.createContainerDevice(userId, "PAR-146", "aca");
		const { router } = makeRouter(store);

		// No affinity, no park. A replayed `active` must not invent ownership for
		// a session this device does not have.
		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "ss-1",
			sessionId: "sess-unknown",
			state: "active",
		});

		expect(store.getSessionAffinity("sess-unknown")).toBeUndefined();
	});

	it("(f) sweepExpired posts the TTL expiry activity and frees an undelivered created event's lock", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router, postActivity, clock } = makeRouter(store);
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);
		postActivity.mockClear();

		clock.value = ROUTE_NOW + TTL_MS + 1;
		await router.sweepExpired();

		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			expiredMessage("alice@example.com"),
		);
		// Undelivered created event → lock + affinity released.
		expect(store.getSessionAffinity("sess-1")).toBeUndefined();
		expect(store.acquireIssueLock("ISS-1", "sess-2", aliceDevice)).toBe(true);
	});

	it("(g) sweepExpired releases a delivered session's lock when the device is dark past the TTL", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const onlineGateway: Gateway = {
			isOnline: () => true,
			deliverPending: vi.fn<(deviceId: number) => void>(),
		};
		const { router, postActivity, clock } = makeRouter(store, {
			gateway: onlineGateway,
		});
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);
		// Delivered while the device was online, and acked — so the queue is
		// empty and pass 1 has nothing to expire. That isolation is the point of
		// the test: the lock can only be released by the stale-lock pass.
		expect(onlineGateway.deliverPending).toHaveBeenCalledWith(aliceDevice);
		store.ackEvent(aliceDevice, 1);

		// Device goes dark a second after the route, and stays dark past the TTL.
		// Both clocks have to age out, not just `last_seen_ms`: darkness that
		// PRECEDES the route is a device with a boot in flight, which CYR-81 must
		// not reclaim. The old fixture set `last_seen_ms` to 900_000 — before the
		// 1_000_000 route it had just asserted was delivered to an online device.
		store.touchDevice(aliceDevice, ROUTE_NOW + 1_000);
		postActivity.mockClear();
		clock.value = ROUTE_NOW + TTL_MS + 1_000 + 1; // cutoff is past both stamps

		await router.sweepExpired();

		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			offlineReleaseMessage("alice@example.com"),
		);
		expect(store.getIssueLock("ISS-1")).toBeUndefined();
		expect(store.acquireIssueLock("ISS-1", "sess-2", aliceDevice)).toBe(true);
	});

	it("(g2) sweepExpired keeps a long-parked container's lock while a boot it was just routed to is in flight (CYR-81)", async () => {
		const { userId } = store.addUser({ email: "alice@example.com" });
		const { deviceId } = store.createContainerDevice(userId, "ISS-1", "aca");
		store.acquireIssueLock("ISS-1", "sess-1", deviceId);
		store.setSessionAffinity("sess-1", deviceId, undefined, ROUTE_NOW);
		const { router, postActivity, clock } = makeRouter(store);

		// The row a parked ACA sandbox presents the instant it is routed to: a
		// heartbeat from long before the cutoff, and a route stamp from a moment
		// ago while its boot is still in flight. The event has not expired, so
		// pass 1 cannot be what releases (or spares) the lock.
		store.touchDevice(deviceId, ROUTE_NOW - 10 * TTL_MS);
		store.enqueueEvent(deviceId, '{"n":1}', ROUTE_NOW, TTL_MS);
		clock.value = ROUTE_NOW + 1_000; // cutoff sits between the two stamps

		await router.sweepExpired();

		// Both rows have to survive, and they fail differently. Losing the lock
		// disowns the session the boot is about to start, so its Linear activity
		// is refused; losing affinity is what lets the idle sweep read zero and
		// suspend the sandbox mid-session. On CAN-170 both happened.
		expect(store.getIssueLock("ISS-1")?.sessionId).toBe("sess-1");
		expect(store.getSessionAffinity("sess-1")).toBe(deviceId);
		expect(postActivity).not.toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			offlineReleaseMessage("alice@example.com"),
		);
	});

	it("(g3) sweepExpired still reclaims a dark PHYSICAL device that was just routed to (CYR-81)", async () => {
		// The counterpart to (g2), and the reason the guard is container-only.
		// `last_routed_ms` is per-device and a laptop serves every issue its
		// owner works, so guarding one here would let a user who keeps being
		// delegated work hold an unrelated issue locked for as long as the
		// delegations keep coming.
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router, clock } = makeRouter(store);

		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);
		expect(store.getIssueLock("ISS-1")?.sessionId).toBe("sess-1");

		// Dark long before the cutoff, routed to a moment ago — the exact row
		// shape (g2) spares for a container.
		store.touchDevice(aliceDevice, ROUTE_NOW - 10 * TTL_MS);
		clock.value = ROUTE_NOW + 1_000;

		await router.sweepExpired();

		expect(store.getIssueLock("ISS-1")).toBeUndefined();
	});

	it("(g4) sweepExpired still reclaims a dead container's acked session once routing stops (CYR-81)", async () => {
		// The container-side proof that the guard defers rather than strands. An
		// acked event is gone from the queue, so pass 1 has nothing to expire and
		// a permanently disconnected container never reaches the terminal-frame
		// or hello-reconciliation paths — pass 2 is the only automatic releaser
		// left, and it must still fire once the route stamp ages out too.
		const { userId } = store.addUser({ email: "alice@example.com" });
		const { deviceId } = store.createContainerDevice(userId, "ISS-1", "aca");
		store.acquireIssueLock("ISS-1", "sess-1", deviceId);
		store.setSessionAffinity("sess-1", deviceId, undefined, ROUTE_NOW);
		const { router, clock } = makeRouter(store);

		const seq = store.enqueueEvent(deviceId, '{"n":1}', ROUTE_NOW, TTL_MS);
		store.ackEvent(deviceId, seq); // delivered, then the container died
		store.touchDevice(deviceId, ROUTE_NOW + 1_000);
		clock.value = ROUTE_NOW + 1_000 + TTL_MS + 1; // both stamps past the cutoff

		await router.sweepExpired();

		expect(store.getIssueLock("ISS-1")).toBeUndefined();
		expect(store.getSessionAffinity("sess-1")).toBeUndefined();
	});
});

describe("EventRouter container routing", () => {
	let store: RouterStore;

	beforeEach(() => {
		store = new RouterStore(":memory:");
	});

	const DAVE: Creator = {
		id: "lin-dave",
		email: "dave@example.com",
		name: "Dave",
	};

	it("(h) routes a created event for a container-executor user to the issue's container device and skips the offline notice", async () => {
		store.addUser({ email: "dave@example.com", linearId: "lin-dave" });
		store.setUserExecutor("dave@example.com", '{"type":"docker"}');
		const { containerTargets, executor, secrets } = makeContainerTargets(store);
		// A genuine claudeOauthToken so boot() actually reaches ensureRunning
		// (a boot-failure notice is a separate, already-covered concern).
		secrets.set("dave@example.com", "claudeOauthToken", "tok-1");
		const bootSpy = vi.spyOn(containerTargets, "boot");
		const { router, postActivity } = makeRouter(store, { containerTargets });

		await router.route(
			createdEvent({
				sessionId: "sess-1",
				issueId: "ISS-1",
				identifier: "CYPACK-1",
				creator: DAVE,
			}),
		);

		const device = store.getContainerDeviceForIssue("CYPACK-1");
		expect(device).toMatchObject({ provider: "docker" });
		expect(
			store.pendingEvents(device?.deviceId ?? -1, 0, ROUTE_NOW),
		).toHaveLength(1);
		expect(bootSpy).toHaveBeenCalledWith(device?.deviceId, {
			workspaceId: "ws-1",
			sessionId: "sess-1",
		});
		// A cold boot is expected, not an outage: the offline notice must never fire.
		expect(postActivity).not.toHaveBeenCalled();

		await vi.waitFor(() =>
			expect(executor.ensureRunning).toHaveBeenCalledTimes(1),
		);
	});

	/**
	 * Regression guard for the 2026-07-27 PAR-166 investigation. Routing a
	 * created event to a container device wrote affinity rows, queued the event
	 * and dispatched a boot — and logged NOTHING. Meanwhile every webhook the
	 * router deliberately ignores logs a line. The console therefore showed only
	 * the events Cyrus did not act on, which read as "Linear never sent the
	 * agent-session event" when in fact it had been received and routed.
	 * Accepting work must be at least as visible as ignoring it.
	 */
	it("(h2) logs acceptance and the container boot dispatch for a routed created event", async () => {
		store.addUser({ email: "dave@example.com", linearId: "lin-dave" });
		store.setUserExecutor("dave@example.com", '{"type":"docker"}');
		const { containerTargets, secrets } = makeContainerTargets(store);
		secrets.set("dave@example.com", "claudeOauthToken", "tok-1");
		const info = vi.fn<(msg: string) => void>();
		// A full logger, not the two methods this assertion reads: routing now also
		// emits `run.routed`, and a stub missing `event` would fail on that instead
		// of on the claim the test is making.
		const logger = testLogger({ info, warn: () => {} });
		const { router } = makeRouter(store, { containerTargets, logger });

		await router.route(
			createdEvent({
				sessionId: "sess-1",
				issueId: "ISS-1",
				identifier: "CYPACK-1",
				creator: DAVE,
			}),
		);

		const logged = info.mock.calls.map(([msg]) => msg).join("\n");
		// The session must be traceable to the device that took it.
		expect(logged).toContain("sess-1");
		// And the container boot must announce itself, so a worker that never
		// connects is visibly a boot that started and did not finish.
		expect(logged.toLowerCase()).toContain("boot");
	});

	it("(i) falls through and heals when session affinity points at a deleted container device", async () => {
		store.addUser({ email: "dave@example.com", linearId: "lin-dave" });
		store.setUserExecutor("dave@example.com", '{"type":"docker"}');
		const { containerTargets, secrets } = makeContainerTargets(store);
		secrets.set("dave@example.com", "claudeOauthToken", "tok-1");
		const { router, postActivity } = makeRouter(store, { containerTargets });

		// Session affinity points at a device id that was never created (as if
		// its container had since been destroyed and the row removed).
		store.setSessionAffinity("sess-1", 999_999);

		await router.route(
			promptedEvent({
				sessionId: "sess-1",
				issueId: "ISS-1",
				identifier: "ISS-1",
				actorUserId: "lin-dave",
				creator: DAVE,
			}),
		);

		// Re-resolved via the creator chain into a fresh container device, not
		// left pointing at the deleted id — and the caller is told nothing was
		// unroutable.
		const device = store.getContainerDeviceForIssue("ISS-1");
		expect(device).toBeDefined();
		expect(store.getSessionAffinity("sess-1")).toBe(device?.deviceId);
		expect(postActivity).not.toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			PROMPT_UNROUTABLE_MESSAGE,
		);
	});

	it("(j) keeps physical-device routing unchanged when containerTargets is not configured", async () => {
		const deviceId = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		// executor_json exists and says "device" — with no containerTargets
		// wired up at all, the container codepath must never even be
		// consulted, and routing must be byte-identical to today.
		store.setUserExecutor("alice@example.com", '{"type":"device"}');
		const { router, postActivity } = makeRouter(store);

		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);

		expect(store.pendingEvents(deviceId, 0, ROUTE_NOW)).toHaveLength(1);
		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			offlineWaitingMessage("alice@example.com"),
		);
	});

	it("(k) refuses to route a malformed issue key into the store instead of crashing or creating a broken container", async () => {
		store.addUser({ email: "dave@example.com", linearId: "lin-dave" });
		store.setUserExecutor("dave@example.com", '{"type":"docker"}');
		const { containerTargets } = makeContainerTargets(store);
		const { router, postActivity } = makeRouter(store, { containerTargets });

		await expect(
			router.route(
				createdEvent({
					sessionId: "sess-1",
					issueId: "ISS-1",
					identifier: "bad issue/key!",
					creator: DAVE,
				}),
			),
		).resolves.toBeUndefined();

		// The gate refused: nothing was created in the store...
		expect(store.listContainerDevices()).toHaveLength(0);
		// ...and the router posts a message about the issue's identifier, NOT
		// the unenrolled-creator message — Dave IS enrolled with a container
		// executor, it's this issue's key that's the problem (Finding 4).
		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			fillTemplate(INVALID_ISSUE_KEY_MESSAGE, { issueKey: "bad issue/key!" }),
		);
		expect(postActivity).not.toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			fillTemplate(UNENROLLED_CREATOR_MESSAGE, { userName: "Dave" }),
		);
	});

	it("(l) posts the same invalid-issue-key message for a prompted event with a malformed identifier, not the generic unroutable message", async () => {
		store.addUser({ email: "dave@example.com", linearId: "lin-dave" });
		store.setUserExecutor("dave@example.com", '{"type":"docker"}');
		const { containerTargets } = makeContainerTargets(store);
		const { router, postActivity } = makeRouter(store, { containerTargets });

		await expect(
			router.route(
				promptedEvent({
					sessionId: "sess-1",
					issueId: "ISS-1",
					identifier: "bad issue/key!",
					actorUserId: "lin-dave",
					creator: DAVE,
				}),
			),
		).resolves.toBeUndefined();

		expect(store.listContainerDevices()).toHaveLength(0);
		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			fillTemplate(INVALID_ISSUE_KEY_MESSAGE, { issueKey: "bad issue/key!" }),
		);
		expect(postActivity).not.toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			PROMPT_UNROUTABLE_MESSAGE,
		);
	});

	it("(m) fails closed for a created event whose webhook carries no issue identifier at all, instead of minting a container keyed by the UUID issueId", async () => {
		// Regression test: extractIssueKey() used to fall back to `issueId` (or
		// `sessionId`) when the webhook's `agentSession.issue.identifier` was
		// missing. Both are Linear-internal UUIDs that pass ISSUE_KEY_RE fine,
		// so the old code would silently create a container device keyed by a
		// UUID the in-container edge worker's floor uploads (keyed by the
		// human-readable `session.issue.identifier`) can never match — every
		// floor upload for that container's life would 403, silently. The fix
		// treats a missing identifier as an invalid issue key up front.
		store.addUser({ email: "dave@example.com", linearId: "lin-dave" });
		store.setUserExecutor("dave@example.com", '{"type":"docker"}');
		const { containerTargets } = makeContainerTargets(store);
		const { router, postActivity } = makeRouter(store, { containerTargets });

		await expect(
			router.route(
				createdEvent({
					sessionId: "sess-1",
					issueId: "11111111-2222-3333-4444-555555555555",
					// identifier deliberately omitted.
					creator: DAVE,
				}),
			),
		).resolves.toBeUndefined();

		expect(store.listContainerDevices()).toHaveLength(0);
		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			fillTemplate(INVALID_ISSUE_KEY_MESSAGE, {
				issueKey: "11111111-2222-3333-4444-555555555555",
			}),
		);
	});

	it("(n) fails closed for a prompted event whose webhook carries no issue identifier at all", async () => {
		store.addUser({ email: "dave@example.com", linearId: "lin-dave" });
		store.setUserExecutor("dave@example.com", '{"type":"docker"}');
		const { containerTargets } = makeContainerTargets(store);
		const { router, postActivity } = makeRouter(store, { containerTargets });

		await expect(
			router.route(
				promptedEvent({
					sessionId: "sess-1",
					issueId: "11111111-2222-3333-4444-555555555555",
					// identifier deliberately omitted.
					actorUserId: "lin-dave",
					creator: DAVE,
				}),
			),
		).resolves.toBeUndefined();

		expect(store.listContainerDevices()).toHaveLength(0);
		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			fillTemplate(INVALID_ISSUE_KEY_MESSAGE, {
				issueKey: "11111111-2222-3333-4444-555555555555",
			}),
		);
		expect(postActivity).not.toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			PROMPT_UNROUTABLE_MESSAGE,
		);
	});
});

describe("EventRouter issue promotion to a started state", () => {
	let store: RouterStore;

	beforeEach(() => {
		store = new RouterStore(":memory:");
	});

	it("promotes the issue once a created event is accepted", async () => {
		enroll(store, "alice@example.com", { linearId: "lin-alice" });
		const { router, moveIssueToStartedState } = makeRouter(store);

		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);

		expect(moveIssueToStartedState).toHaveBeenCalledTimes(1);
		expect(moveIssueToStartedState).toHaveBeenCalledWith("ws-1", "ISS-1");
	});

	it("promotes even when the target device is offline (the event is queued, not dropped)", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router, moveIssueToStartedState } = makeRouter(store, {
			gateway: { isOnline: () => false, deliverPending: vi.fn() },
		});

		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);

		expect(store.pendingEvents(aliceDevice, 0, ROUTE_NOW)).toHaveLength(1);
		expect(moveIssueToStartedState).toHaveBeenCalledWith("ws-1", "ISS-1");
	});

	it("does not promote an issue whose creator has no enrolled device", async () => {
		const { router, moveIssueToStartedState } = makeRouter(store);

		await router.route(
			createdEvent({
				sessionId: "sess-1",
				issueId: "ISS-1",
				creator: { id: "lin-charlie", email: "c@example.com", name: "Charlie" },
			}),
		);

		expect(moveIssueToStartedState).not.toHaveBeenCalled();
	});

	it("does not promote an issue whose lock is held by another session", async () => {
		enroll(store, "alice@example.com", { linearId: "lin-alice" });
		enroll(store, "bob@example.com", { linearId: "lin-bob" });
		const { router, moveIssueToStartedState } = makeRouter(store);

		await router.route(
			createdEvent({ sessionId: "sess-a", issueId: "ISS-1", creator: ALICE }),
		);
		moveIssueToStartedState.mockClear();

		await router.route(
			createdEvent({ sessionId: "sess-b", issueId: "ISS-1", creator: BOB }),
		);

		expect(moveIssueToStartedState).not.toHaveBeenCalled();
	});

	it("does not promote on a prompted event (the issue is already started)", async () => {
		enroll(store, "alice@example.com", { linearId: "lin-alice" });
		const { router, moveIssueToStartedState } = makeRouter(store);

		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);
		moveIssueToStartedState.mockClear();

		await router.route(
			promptedEvent({
				sessionId: "sess-1",
				issueId: "ISS-1",
				actorUserId: "lin-alice",
				creator: ALICE,
			}),
		);

		expect(moveIssueToStartedState).not.toHaveBeenCalled();
	});

	it("still delivers the event when promotion fails", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router, moveIssueToStartedState } = makeRouter(store);
		moveIssueToStartedState.mockRejectedValueOnce(new Error("Linear is down"));

		await expect(
			router.route(
				createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
			),
		).resolves.toBeUndefined();

		expect(store.pendingEvents(aliceDevice, 0, ROUTE_NOW)).toHaveLength(1);
		expect(store.getSessionAffinity("sess-1")).toBe(aliceDevice);
	});

	it("skips promotion for a session with no issue", async () => {
		enroll(store, "alice@example.com", { linearId: "lin-alice" });
		const { router, moveIssueToStartedState } = makeRouter(store);

		await router.route(createdEvent({ sessionId: "sess-1", creator: ALICE }));

		expect(moveIssueToStartedState).not.toHaveBeenCalled();
	});

	describe("terminal-state webhooks (worktree cleanup)", () => {
		it("classifies container terminal actions, dedupes wakeups, and keeps relaying raw events", async () => {
			const { userId } = store.addUser({
				email: "alice@example.com",
				linearId: ALICE.id,
			});
			const { deviceId } = store.createContainerDevice(
				userId,
				"TEST-1",
				"docker",
			);
			store.setIssueAffinity("issue-1", deviceId);
			const targets = makeContainerTargets(store).containerTargets;
			const wake = vi
				.spyOn(targets, "bootForTeardown")
				.mockImplementation(() => {});
			const register = vi
				.fn()
				.mockReturnValueOnce(true)
				.mockReturnValueOnce(false)
				.mockReturnValueOnce(true);
			const teardown = { register } as unknown as TerminalTeardown;
			const { router } = makeRouter(store, {
				containerTargets: targets,
				terminalTeardown: teardown,
			});

			// Two DISTINCT closed notifications (Done → reopen → Done), hence two
			// payload timestamps: this test is about TerminalTeardown.register's
			// own wakeup dedupe, not the webhook-redelivery gate in route().
			await router.route(
				issueStatusChangedEvent({ issueId: "issue-1", identifier: "TEST-1" }),
			);
			await router.route(
				issueStatusChangedEvent({
					issueId: "issue-1",
					identifier: "TEST-1",
					createdAtMs: ROUTE_NOW + 1_000,
				}),
			);
			await router.route(
				issueDeletedEvent({ issueId: "issue-1", identifier: "TEST-1" }),
			);

			expect(register.mock.calls).toEqual([
				[{ issueKey: "TEST-1", deviceId, action: "closed" }],
				[{ issueKey: "TEST-1", deviceId, action: "closed" }],
				[{ issueKey: "TEST-1", deviceId, action: "deleted" }],
			]);
			expect(wake).toHaveBeenCalledTimes(2);
			expect(store.pendingEvents(deviceId, 0, ROUTE_NOW)).toHaveLength(3);
		});

		it("only relays to physical devices without pending teardown or wake semantics", async () => {
			const deviceId = enroll(store, "alice@example.com", {
				linearId: ALICE.id,
			});
			store.setIssueAffinity("issue-1", deviceId);
			const targets = makeContainerTargets(store).containerTargets;
			const wake = vi.spyOn(targets, "bootForTeardown");
			const register = vi.fn(() => true);
			const { router } = makeRouter(store, {
				containerTargets: targets,
				terminalTeardown: { register } as unknown as TerminalTeardown,
			});

			await router.route(issueStatusChangedEvent({ issueId: "issue-1" }));
			expect(store.pendingEvents(deviceId, 0, ROUTE_NOW)).toHaveLength(1);
			expect(register).not.toHaveBeenCalled();
			expect(wake).not.toHaveBeenCalled();
		});

		it("removes only the retained issue bundle when delete arrives after close teardown", async () => {
			const { userId } = store.addUser({ email: "alice@example.com" });
			const { deviceId } = store.createContainerDevice(
				userId,
				"TEST-1",
				"docker",
			);
			store.setIssueAffinity("issue-1", deviceId);
			const artifactsDir = mkdtempSync(join(tmpdir(), "router-delete-bundle-"));
			const bundle = join(artifactsDir, "TEST-1", "bundle.tar.gz");
			mkdirSync(join(artifactsDir, "TEST-1"));
			writeFileSync(bundle, "retained");
			const destroy = vi.fn(async () => {});
			const teardown = new TerminalTeardown({
				store,
				executors: new Map([
					[
						"docker",
						{
							provider: "docker",
							ensureRunning: vi.fn(async () => {}),
							stop: vi.fn(async () => {}),
							destroy,
							status: vi.fn(async () => "running" as const),
							listManaged: vi.fn(async () => []),
						},
					],
				]),
				artifactsDir,
				graceMs: 60_000,
				logger: silentLogger(),
				setTimeout: () => 1,
				clearTimeout: () => {},
			});
			const targets = makeContainerTargets(store).containerTargets;
			vi.spyOn(targets, "bootForTeardown").mockImplementation(() => {});
			const { router } = makeRouter(store, {
				containerTargets: targets,
				terminalTeardown: teardown,
			});

			await router.route(
				issueStatusChangedEvent({ issueId: "issue-1", identifier: "TEST-1" }),
			);
			await teardown.handleCallback("TEST-1", deviceId);
			expect(existsSync(bundle)).toBe(true);
			expect(store.getIssueAffinity("issue-1")).toBeUndefined();

			await router.route(
				issueDeletedEvent({ issueId: "issue-1", identifier: "TEST-1" }),
			);

			expect(existsSync(bundle)).toBe(false);
			expect(destroy).toHaveBeenCalledTimes(1);
			expect(store.getContainerDeviceForIssue("TEST-1")).toBeUndefined();
		});
		it("forwards an issueStatusChanged webhook to the device holding the issue", async () => {
			const deviceId = enroll(store, "alice@example.com", {
				linearId: ALICE.id,
			});
			const { router, gateway } = makeRouter(store, {
				gateway: {
					isOnline: () => true,
					deliverPending: vi.fn<(deviceId: number) => void>(),
				},
			});

			// Establish issue affinity the same way real traffic does.
			await router.route(
				createdEvent({
					sessionId: "sess-1",
					issueId: "issue-1",
					creator: ALICE,
				}),
			);
			const queuedBefore = store.pendingEvents(deviceId, 0, ROUTE_NOW).length;

			await router.route(issueStatusChangedEvent({ issueId: "issue-1" }));

			const pending = store.pendingEvents(deviceId, 0, ROUTE_NOW);
			expect(pending).toHaveLength(queuedBefore + 1);
			const forwarded = JSON.parse(
				pending[pending.length - 1].payloadJson,
			) as Record<string, unknown>;
			expect(forwarded.type).toBe("AppUserNotification");
			expect(forwarded.action).toBe("issueStatusChanged");
			expect(gateway.deliverPending).toHaveBeenCalledWith(deviceId);
		});

		it("forwards an Issue/remove webhook to the device holding the issue", async () => {
			const deviceId = enroll(store, "alice@example.com", {
				linearId: ALICE.id,
			});
			const { router } = makeRouter(store);

			await router.route(
				createdEvent({
					sessionId: "sess-1",
					issueId: "issue-1",
					creator: ALICE,
				}),
			);
			const queuedBefore = store.pendingEvents(deviceId, 0, ROUTE_NOW).length;

			await router.route(issueDeletedEvent({ issueId: "issue-1" }));

			const pending = store.pendingEvents(deviceId, 0, ROUTE_NOW);
			expect(pending).toHaveLength(queuedBefore + 1);
			const forwarded = JSON.parse(
				pending[pending.length - 1].payloadJson,
			) as Record<string, unknown>;
			expect(forwarded.type).toBe("Issue");
			expect(forwarded.action).toBe("remove");
		});

		it("still forwards after the session ended, when only issue affinity remains", async () => {
			// The real-world case: a session completes (releasing its lock and
			// session affinity) hours before a human moves the issue to Done.
			// Only issue_affinity survives that gap — cleanup must route on it.
			const deviceId = enroll(store, "alice@example.com", {
				linearId: ALICE.id,
			});
			const { router } = makeRouter(store);

			await router.route(
				createdEvent({
					sessionId: "sess-1",
					issueId: "issue-1",
					creator: ALICE,
				}),
			);
			router.handleSessionState(deviceId, {
				type: "session_state",
				sessionId: "sess-1",
				state: "complete",
			} as never);
			expect(store.getSessionAffinity("sess-1")).toBeUndefined();
			const queuedBefore = store.pendingEvents(deviceId, 0, ROUTE_NOW).length;

			await router.route(issueStatusChangedEvent({ issueId: "issue-1" }));

			expect(store.pendingEvents(deviceId, 0, ROUTE_NOW)).toHaveLength(
				queuedBefore + 1,
			);
		});

		it("queues the cleanup for an offline device instead of dropping it", async () => {
			const deviceId = enroll(store, "alice@example.com", {
				linearId: ALICE.id,
			});
			const { router, gateway } = makeRouter(store); // isOnline: () => false

			await router.route(
				createdEvent({
					sessionId: "sess-1",
					issueId: "issue-1",
					creator: ALICE,
				}),
			);
			const queuedBefore = store.pendingEvents(deviceId, 0, ROUTE_NOW).length;

			await router.route(issueStatusChangedEvent({ issueId: "issue-1" }));

			// Queued for replay on reconnect; no delivery attempted while offline.
			expect(store.pendingEvents(deviceId, 0, ROUTE_NOW)).toHaveLength(
				queuedBefore + 1,
			);
			expect(gateway.deliverPending).not.toHaveBeenCalled();
		});

		it("does not post a Linear activity for terminal webhooks (no session thread to post to)", async () => {
			enroll(store, "alice@example.com", { linearId: ALICE.id });
			const { router, postActivity } = makeRouter(store);

			await router.route(
				createdEvent({
					sessionId: "sess-1",
					issueId: "issue-1",
					creator: ALICE,
				}),
			);
			postActivity.mockClear();

			await router.route(issueStatusChangedEvent({ issueId: "issue-1" }));

			expect(postActivity).not.toHaveBeenCalled();
		});

		it("drops a terminal webhook for an issue no device ever worked", async () => {
			const deviceId = enroll(store, "alice@example.com", {
				linearId: ALICE.id,
			});
			const { router } = makeRouter(store);

			await router.route(issueStatusChangedEvent({ issueId: "issue-unknown" }));

			expect(store.pendingEvents(deviceId, 0, ROUTE_NOW)).toHaveLength(0);
		});

		it("drops a terminal webhook carrying no issue id", async () => {
			const deviceId = enroll(store, "alice@example.com", {
				linearId: ALICE.id,
			});
			const { router } = makeRouter(store);

			await router.route(
				createdEvent({
					sessionId: "sess-1",
					issueId: "issue-1",
					creator: ALICE,
				}),
			);
			const queuedBefore = store.pendingEvents(deviceId, 0, ROUTE_NOW).length;

			await router.route({
				type: "AppUserNotification",
				action: "issueStatusChanged",
				organizationId: "ws-1",
				notification: {},
			} as unknown as AgentEvent);

			expect(store.pendingEvents(deviceId, 0, ROUTE_NOW)).toHaveLength(
				queuedBefore,
			);
		});
	});
});

describe("EventRouter reconcileDeviceLocks", () => {
	let store: RouterStore;

	beforeEach(() => {
		store = new RouterStore(":memory:");
	});

	/**
	 * Locks a device via a routed created event, then simulates the device
	 * having received (acked) the queued event so it is "caught up" and its
	 * declared session list is authoritative.
	 */
	async function lockAndDeliver(
		router: EventRouter,
		deviceId: number,
		opts: { sessionId: string; issueId: string },
	): Promise<void> {
		await router.route(
			createdEvent({
				sessionId: opts.sessionId,
				issueId: opts.issueId,
				creator: ALICE,
			}),
		);
		// The created event was queued at seq 1; acking it deletes it, mirroring
		// a device that has processed everything the router sent.
		store.ackEvent(deviceId, 1);
	}

	it("reclaims a lock the reconnecting device no longer tracks", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router, postActivity } = makeRouter(store);
		await lockAndDeliver(router, aliceDevice, {
			sessionId: "sess-1",
			issueId: "ISS-1",
		});

		// Device reconnects declaring it no longer has sess-1 (state was lost).
		await router.reconcileDeviceLocks(aliceDevice, []);

		expect(store.getIssueLock("ISS-1")).toBeUndefined();
		expect(store.getSessionAffinity("sess-1")).toBeUndefined();
		expect(store.acquireIssueLock("ISS-1", "sess-2", aliceDevice)).toBe(true);
		// Workspace hint is still in memory, so the courtesy post goes out.
		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			ORPHANED_LOCK_RECLAIMED_MESSAGE,
		);
	});

	it("keeps a lock the device still declares", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router, postActivity } = makeRouter(store);
		await lockAndDeliver(router, aliceDevice, {
			sessionId: "sess-1",
			issueId: "ISS-1",
		});
		postActivity.mockClear();

		await router.reconcileDeviceLocks(aliceDevice, ["sess-1"]);

		// Still locked: a different session cannot take it.
		expect(store.acquireIssueLock("ISS-1", "sess-2", aliceDevice)).toBe(false);
		expect(postActivity).not.toHaveBeenCalled();
	});

	it("defers when the device still has undelivered events", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router } = makeRouter(store);
		// Route but do NOT ack: the created event is still queued, so the device
		// isn't caught up and its (empty) declared set is not yet authoritative.
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);

		await router.reconcileDeviceLocks(aliceDevice, []);

		// Lock preserved — reconciliation must not race the pending created event.
		expect(store.acquireIssueLock("ISS-1", "sess-2", aliceDevice)).toBe(false);
	});

	it("is a no-op for an older client that declares no list (undefined)", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router, postActivity } = makeRouter(store);
		await lockAndDeliver(router, aliceDevice, {
			sessionId: "sess-1",
			issueId: "ISS-1",
		});
		postActivity.mockClear();

		await router.reconcileDeviceLocks(aliceDevice, undefined);

		expect(store.acquireIssueLock("ISS-1", "sess-2", aliceDevice)).toBe(false);
		expect(postActivity).not.toHaveBeenCalled();
	});

	it("still releases the lock after a router restart even though it cannot post", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		// Router A acquires the lock, then the device acks the event.
		const { router: routerA } = makeRouter(store);
		await lockAndDeliver(routerA, aliceDevice, {
			sessionId: "sess-1",
			issueId: "ISS-1",
		});

		// Router B is a fresh process sharing the same store: its in-memory
		// sessionWorkspace map is empty (the PAR-110 scenario). It can't address
		// the Linear thread, but must still free the lock.
		const { router: routerB, postActivity: postB } = makeRouter(store);
		await routerB.reconcileDeviceLocks(aliceDevice, []);

		expect(store.getIssueLock("ISS-1")).toBeUndefined();
		expect(store.acquireIssueLock("ISS-1", "sess-2", aliceDevice)).toBe(true);
		expect(postB).not.toHaveBeenCalled();
	});

	it("does nothing when issue locking is disabled", async () => {
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		const { router } = makeRouter(store, { config: { issueLock: false } });
		// No lock is taken when issueLock is off; reconcile must simply return.
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);
		store.ackEvent(aliceDevice, 1);

		await expect(
			router.reconcileDeviceLocks(aliceDevice, []),
		).resolves.toBeUndefined();
	});
});

describe("EventRouter reconcileDeviceAffinity", () => {
	const GRACE = 600_000;
	let store: RouterStore;
	let router: EventRouter;
	let aliceDevice: number;

	beforeEach(() => {
		store = new RouterStore(":memory:");
		aliceDevice = enroll(store, "alice@example.com", { linearId: "lin-alice" });
		({ router } = makeRouter(store, { config: { affinityGraceMs: GRACE } }));
	});

	it("reclaims an undeclared row older than the grace window", () => {
		// The PAR-146 shape: affinity for a session that already went terminal,
		// with no issue lock, because routePrompted sets affinity without one.
		store.setSessionAffinity("dead-sess", aliceDevice, undefined, 1_000);

		const remaining = router.reconcileDeviceAffinity(
			aliceDevice,
			[],
			1_000 + GRACE + 1,
		);

		expect(remaining).toBe(0);
		expect(store.countSessionAffinityForDevice(aliceDevice)).toBe(0);
	});

	it("keeps an undeclared row still inside the grace window", () => {
		// Just routed: the worker has the event queued but has not started
		// tracking the session yet, so it cannot declare it.
		store.setSessionAffinity("fresh-sess", aliceDevice, undefined, 1_000);

		const remaining = router.reconcileDeviceAffinity(
			aliceDevice,
			[],
			1_000 + GRACE - 1,
		);

		expect(remaining).toBe(1);
		expect(store.countSessionAffinityForDevice(aliceDevice)).toBe(1);
	});

	it("keeps a declared row no matter how old it is", () => {
		// A session that has legitimately been working for hours.
		store.setSessionAffinity("live-sess", aliceDevice, undefined, 1_000);

		const remaining = router.reconcileDeviceAffinity(
			aliceDevice,
			["live-sess"],
			1_000 + GRACE * 1_000,
		);

		expect(remaining).toBe(1);
		expect(store.countSessionAffinityForDevice(aliceDevice)).toBe(1);
	});

	it("reclaims nothing when the device declared no list", () => {
		store.setSessionAffinity("dead-sess", aliceDevice, undefined, 1_000);

		const remaining = router.reconcileDeviceAffinity(
			aliceDevice,
			undefined,
			1_000 + GRACE + 1,
		);

		expect(remaining).toBe(1);
		expect(store.countSessionAffinityForDevice(aliceDevice)).toBe(1);
	});

	it("preserves the creator gate for rows it keeps", () => {
		store.setSessionAffinity(
			"live-sess",
			aliceDevice,
			JSON.stringify({ id: "u1" }),
			1_000,
		);

		router.reconcileDeviceAffinity(
			aliceDevice,
			["live-sess"],
			1_000 + GRACE + 1,
		);

		expect(store.getSessionCreator("live-sess")).toBe(
			JSON.stringify({ id: "u1" }),
		);
	});
});

describe("EventRouter dangling issue/parent affinity healing", () => {
	let store: RouterStore;

	beforeEach(() => {
		store = new RouterStore(":memory:");
	});

	it("heals a dangling issue-affinity row instead of routing into the void", async () => {
		// No session affinity, no user enrolled that matches the creator, but a
		// stale issue_affinity row pointing at a device_id that was never
		// created — as if `revokeDevice` deleted the devices row (it does not
		// purge issue_affinity; see RouterStore.revokeDevice).
		store.setIssueAffinity("ISS-1", 999_999);
		const { router, postActivity } = makeRouter(store);

		// Before the fix this would resolve to { deviceId: 999_999, kind:
		// "device" } and enqueueEvent() would throw "Unknown device: 999999",
		// rejecting out of route() with no .catch() upstream (RouterServer) —
		// an unhandled rejection that takes the whole router process down.
		await expect(
			router.route(
				promptedEvent({
					sessionId: "sess-1",
					issueId: "ISS-1",
					actorUserId: "lin-bob",
					creator: BOB,
				}),
			),
		).resolves.toBeUndefined();

		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			PROMPT_UNROUTABLE_MESSAGE,
		);
		// The stale row was cleared, not left dangling for the next event.
		expect(store.getIssueAffinity("ISS-1")).toBeUndefined();
	});

	it("heals a dangling parent-issue-affinity row instead of routing into the void", async () => {
		store.setIssueAffinity("ISS-parent", 999_999);
		const { router, postActivity } = makeRouter(store);
		const creator: Creator = {
			id: "lin-x",
			email: "x@example.com",
			name: "X",
		};

		await expect(
			router.route(
				createdEvent({
					sessionId: "sess-1",
					issueId: "ISS-child",
					parentIssueId: "ISS-parent",
					creator,
				}),
			),
		).resolves.toBeUndefined();

		expect(postActivity).toHaveBeenCalledWith(
			"ws-1",
			"sess-1",
			fillTemplate(UNENROLLED_CREATOR_MESSAGE, { userName: "X" }),
		);
		expect(store.getIssueAffinity("ISS-parent")).toBeUndefined();
	});

	it("re-resolves through a healthy issue-affinity row once it points at a live device (regression guard)", async () => {
		// A live issue-affinity row must still work exactly as before — the
		// healing branch must only fire when getDeviceInfo() is undefined.
		const aliceDevice = enroll(store, "alice@example.com", {
			linearId: "lin-alice",
		});
		store.setIssueAffinity("ISS-1", aliceDevice);
		const { router, postActivity } = makeRouter(store);

		await router.route(
			promptedEvent({
				sessionId: "sess-new",
				issueId: "ISS-1",
				actorUserId: "lin-charlie",
				creator: { id: "lin-charlie", email: "charlie@example.com", name: "C" },
			}),
		);

		expect(store.pendingEvents(aliceDevice, 0, ROUTE_NOW)).toHaveLength(1);
		expect(postActivity).not.toHaveBeenCalledWith(
			"ws-1",
			"sess-new",
			PROMPT_UNROUTABLE_MESSAGE,
		);
	});
});

/**
 * CYR-68 — the worker reports what a run is doing, explicitly, and the router
 * records it without inferring anything from silence.
 *
 * The failure being designed out: `parked` conflated two independent facts —
 * that a run is blocked on a user answer, and that its container may be
 * suspended — so a run held open by a live background build was reported as
 * nothing at all, and a run waiting on a user was indistinguishable from a
 * container that had been idle-stopped.
 */
describe("EventRouter explicit run facts", () => {
	let store: RouterStore;

	beforeEach(() => {
		store = new RouterStore(":memory:");
	});

	/** Routes a created event so the device owns the session and a run exists. */
	async function routedSession(overrides?: {
		identifier?: string;
		team?: { id: string; name: string };
		fetchRoutingContext?: EventRouterOptions["fetchRoutingContext"];
	}) {
		const deviceId = enroll(store, ALICE.email, {
			name: ALICE.name,
			linearId: ALICE.id,
		});
		const { router, clock } = makeRouter(store, {
			...(overrides?.fetchRoutingContext
				? { fetchRoutingContext: overrides.fetchRoutingContext }
				: {}),
		});
		const event = createdEvent({
			sessionId: "sess-1",
			issueId: "ISS-1",
			identifier: overrides?.identifier ?? "NOR-1",
			creator: ALICE,
		}) as unknown as {
			agentSession: { issue: Record<string, unknown> };
		};
		if (overrides?.team) {
			event.agentSession.issue.team = overrides.team;
			event.agentSession.issue.teamId = overrides.team.id;
		}
		await router.route(event as unknown as AgentEvent);
		return { router, deviceId, clock };
	}

	function run() {
		const found = store.listAgentRuns({ userId: 1 })[0];
		if (!found) throw new Error("expected a run");
		return found;
	}

	it("records an explicit elicitation wait and parks the executor when told it may", async () => {
		const { router, deviceId } = await routedSession();

		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "waiting",
			wait: { reason: "elicitation", since: new Date(ROUTE_NOW).toISOString() },
			executorMayPark: true,
			runner: "claude",
			model: "claude-opus-5",
		});

		expect(run()).toMatchObject({
			state: "waiting",
			wait: { reason: "elicitation", sinceMs: ROUTE_NOW },
			runner: "claude",
			model: "claude-opus-5",
		});
		// Park permission is what releases affinity, so `ContainerLifecycle` can
		// idle-stop the container while the user thinks.
		expect(store.countSessionAffinityForDevice(deviceId)).toBe(0);
	});

	it("records a wait that must NOT park its executor without releasing affinity", async () => {
		const { router, deviceId } = await routedSession();

		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "waiting",
			wait: { reason: "elicitation", since: new Date(ROUTE_NOW).toISOString() },
			pendingWorkCount: 1,
		});

		// The run is blocked either way — that is the worker's report, and it is
		// recorded. What changes is the executor: suspending a container with a
		// build in flight freezes it, and the completion that would wake the
		// session could then never arrive.
		expect(run()).toMatchObject({
			state: "waiting",
			wait: { reason: "elicitation" },
			pendingWorkCount: 1,
		});
		expect(store.countSessionAffinityForDevice(deviceId)).toBe(1);
	});

	it("records an explicit `other` wait with the condition the worker reported", async () => {
		const { router, deviceId } = await routedSession();

		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "waiting",
			wait: {
				reason: "other",
				since: new Date(ROUTE_NOW).toISOString(),
				reportedCondition: "waiting on a deploy lock",
			},
		});

		expect(run().wait).toEqual({
			reason: "other",
			sinceMs: ROUTE_NOW,
			reportedCondition: "waiting on a deploy lock",
		});
		// A wait the schema does not model says nothing about the executor.
		expect(store.countSessionAffinityForDevice(deviceId)).toBe(1);
	});

	it("reads a legacy parked frame as waiting-on-elicitation plus executor parking", async () => {
		const { router, deviceId } = await routedSession();

		// Exactly what a pre-run-facts worker sends: no wait, no facts, nothing.
		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "parked",
		});

		expect(run()).toMatchObject({
			state: "waiting",
			wait: { reason: "elicitation", sinceMs: ROUTE_NOW },
		});
		// The executor half of what `parked` always meant, unchanged.
		expect(store.countSessionAffinityForDevice(deviceId)).toBe(0);

		// And the legacy unpark still restores affinity through the park record.
		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f2",
			sessionId: "sess-1",
			state: "active",
		});
		expect(run().state).toBe("active");
		expect(run().wait).toBeUndefined();
		expect(store.countSessionAffinityForDevice(deviceId)).toBe(1);
	});

	it("keeps a pending-work run active without minting any claim", async () => {
		const { router, deviceId } = await routedSession();

		// The seven-hour cron case: no park was ever recorded, so there is nothing
		// to redeem — but the run is demonstrably working and its count is what
		// makes that legible.
		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "active",
			pendingWorkCount: 4,
			runner: "claude",
		});

		expect(run()).toMatchObject({
			state: "active",
			pendingWorkCount: 4,
			runner: "claude",
		});
		expect(run().wait).toBeUndefined();
		// Unchanged from the route: the frame granted nothing.
		expect(store.countSessionAffinityForDevice(deviceId)).toBe(1);
	});

	it("refuses to label a run waiting for a device that does not own it", async () => {
		const { router } = await routedSession();
		const mallory = enroll(store, "mallory@example.com");

		router.handleSessionState(mallory, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "waiting",
			wait: { reason: "elicitation", since: new Date(ROUTE_NOW).toISOString() },
		});

		// `setAgentRunState` writes to whatever session the frame names, so an
		// ungated path would let any enrolled device label someone else's run.
		expect(run().state).toBe("routed");
		expect(run().wait).toBeUndefined();
	});

	it("ignores an active frame from a device that owns nothing", async () => {
		const { router } = await routedSession();
		const mallory = enroll(store, "mallory@example.com");

		router.handleSessionState(mallory, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "active",
			pendingWorkCount: 9,
		});

		expect(run().pendingWorkCount).toBeUndefined();
	});

	it("narrows a wait reason it does not model to `other`, keeping the raw text", async () => {
		const { router, deviceId } = await routedSession();

		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "waiting",
			wait: {
				reason: "quota_backoff",
				since: new Date(ROUTE_NOW).toISOString(),
			},
		});

		// Criterion (b): unknown worker reasons are `other`. The narrowing happens
		// here rather than in the frame schema, whose only way to reject a reason
		// is a parse failure — which `DeviceGateway` answers by closing the whole
		// device socket. The raw reason survives as the condition, so narrowing
		// loses the classification and never the fact.
		expect(run().wait).toEqual({
			reason: "other",
			sinceMs: ROUTE_NOW,
			reportedCondition: "unmodelled worker wait reason: quota_backoff",
		});
	});

	it("supplies a condition for an `other` wait that arrived without one", async () => {
		const { router, deviceId } = await routedSession();

		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "waiting",
			wait: { reason: "other", since: new Date(ROUTE_NOW).toISOString() },
		});

		// An `other` wait with no text records nothing an operator could act on,
		// and the v1 observation refuses it — but "the worker declined to say" is
		// itself the actionable fact, and it beats closing the socket over it.
		expect(run().wait?.reason).toBe("other");
		expect(run().wait?.reportedCondition).toBe(
			"the worker reported `other` without describing the condition",
		);
	});

	it("records the runner and model a terminal frame reports", async () => {
		const { router, deviceId } = await routedSession();

		// The ordinary run: no elicitation, no deferred pending work. Its terminal
		// frame is the ONLY point at which execution identity is ever offered, so
		// dropping it here left the common case with `runner = NULL` forever — a
		// gap no later backfill could close, since the worker is gone.
		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "complete",
			runner: "codex",
			model: "gpt-5.5-codex",
		});

		expect(run()).toMatchObject({
			state: "complete",
			runner: "codex",
			model: "gpt-5.5-codex",
		});
	});

	it("fills the project dimension the webhook cannot carry", async () => {
		// Linear's `AgentSessionEventWebhookPayload.issue` has no project field at
		// any nesting, so without this read the project columns could never be
		// filled and the filter they exist for would match zero runs.
		const fetchRoutingContext = vi.fn(async () => ({
			linearTeamId: "team-1",
			linearTeamName: "Platform",
			linearProjectId: "proj-1",
			linearProjectName: "Observability",
		}));
		await routedSession({ fetchRoutingContext });
		await vi.waitFor(() =>
			expect(run().routing.linearProjectId).toBe("proj-1"),
		);

		expect(run().routing).toMatchObject({
			linearTeamId: "team-1",
			linearTeamName: "Platform",
			linearProjectId: "proj-1",
			linearProjectName: "Observability",
		});
		expect(fetchRoutingContext).toHaveBeenCalledWith("ws-1", "ISS-1");
	});

	it("never lets a later read rewrite a dimension captured at route time", async () => {
		// The issue has already moved by the time the enriching read lands. The
		// snapshot's whole value is that it does not move, so a late read may fill
		// a blank and never overwrite.
		const fetchRoutingContext = vi.fn(async () => ({
			linearTeamId: "team-2",
			linearTeamName: "Infra",
			linearProjectId: "proj-1",
		}));
		await routedSession({
			team: { id: "team-1", name: "Platform" },
			fetchRoutingContext,
		});
		await vi.waitFor(() =>
			expect(run().routing.linearProjectId).toBe("proj-1"),
		);

		expect(run().routing.linearTeamId).toBe("team-1");
		expect(run().routing.linearTeamName).toBe("Platform");
	});

	it("asks Linear for the routing context once per run, not once per webhook", async () => {
		// An issue in NO project answers with nothing to fill. Without a "have we
		// asked?" stamp that reads as "still needs enrichment", and every later
		// prompt into the same run pays another Linear round-trip forever — the
		// commonest case being the most expensive one.
		const fetchRoutingContext = vi.fn(async () => ({
			linearTeamId: "team-1",
		}));
		const { router } = await routedSession({ fetchRoutingContext });
		await vi.waitFor(() =>
			expect(fetchRoutingContext).toHaveBeenCalledTimes(1),
		);

		await router.route(
			promptedEvent({
				sessionId: "sess-1",
				issueId: "ISS-1",
				actorUserId: ALICE.id,
				creator: ALICE,
			}),
		);
		await vi.waitFor(() => expect(run().inputs).toHaveLength(2));

		expect(fetchRoutingContext).toHaveBeenCalledTimes(1);
	});

	it("routes normally when the routing-context read fails", async () => {
		const fetchRoutingContext = vi.fn(async () => {
			throw new Error("Linear is down");
		});
		const { deviceId } = await routedSession({ fetchRoutingContext });

		// A snapshot dimension is worth a best-effort read and never worth failing
		// a route over: the event that starts the work is already queued.
		await vi.waitFor(() => expect(fetchRoutingContext).toHaveBeenCalled());
		expect(store.pendingEvents(deviceId, 0, ROUTE_NOW)).toHaveLength(1);
		expect(run().state).toBe("routed");
	});

	it("keeps a rate limit terminal rather than turning it into a wait", async () => {
		const { router, deviceId, clock } = await routedSession();

		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "waiting",
			wait: { reason: "elicitation", since: new Date(ROUTE_NOW).toISOString() },
			pendingWorkCount: 2,
		});

		// The wait enum has no rate-limit member, deliberately: a rate limit ENDS
		// the run today, and there is no resumable-backoff design behind it that
		// would make "waiting on a rate limit" a state anything could act on. So a
		// rate-limited run arrives here as a terminal `error`.
		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f2",
			sessionId: "sess-1",
			state: "error",
			runner: "claude",
		});

		expect(run()).toMatchObject({ state: "error", endedMs: clock.value });
		expect(run().wait).toBeUndefined();
		// Live background work under a run that has ENDED is a contradiction.
		expect(run().pendingWorkCount).toBeUndefined();
	});

	it("captures the routing snapshot at route time and holds it steady", async () => {
		const { router } = await routedSession({
			team: { id: "team-1", name: "Platform" },
		});

		expect(run().routing).toMatchObject({
			workspaceId: "ws-1",
			ownerUserId: "1",
			ownerName: "Alice",
			linearTeamId: "team-1",
			linearTeamName: "Platform",
			routedAtMs: ROUTE_NOW,
		});
		expect(run().issueId).toBe("ISS-1");

		// The issue moves team mid-run. A historical filter must keep finding the
		// run under the team it was ROUTED under.
		const moved = createdEvent({
			sessionId: "sess-1",
			issueId: "ISS-1",
			identifier: "NOR-1",
			creator: ALICE,
		}) as unknown as { agentSession: { issue: Record<string, unknown> } };
		moved.agentSession.issue.team = { id: "team-2", name: "Infra" };
		moved.agentSession.issue.teamId = "team-2";
		await router.route(moved as unknown as AgentEvent);

		expect(run().routing.linearTeamId).toBe("team-1");
		expect(run().routing.linearTeamName).toBe("Platform");
	});

	it("increments the revision only when a material fact changes", async () => {
		const { router, deviceId } = await routedSession();
		const revision = () => run().revision;
		const before = revision();

		const waiting = {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "waiting",
			wait: { reason: "elicitation", since: new Date(ROUTE_NOW).toISOString() },
			pendingWorkCount: 1,
		} as const;
		router.handleSessionState(deviceId, waiting);
		expect(revision()).toBe(before + 1);

		// A replayed frame — the router applies `session_state` before acking it,
		// so a device that dies in between resends one it already applied. An
		// idempotent replay must not look like a change to a watching client.
		router.handleSessionState(deviceId, { ...waiting, id: "f2" });
		expect(revision()).toBe(before + 1);
	});
});

/**
 * CYR-72: the canonical attribution has to be the SAME set of facts at every
 * point in a run's life.
 *
 * The failure being designed out is not a missing attribute — it is two
 * emitters that each know a different half. Before this, a route logged a
 * device and an issue, a terminal frame logged a session id, and neither knew
 * the workspace, owner, team or project; an operator answering "what happened
 * to this run?" had to join three partial views by hand and could only do it if
 * they already knew the session id to start from. These tests assert the join
 * key survives from route through terminal and through the recovery paths,
 * because that is the property a query depends on and the one a refactor of any
 * single emitter would silently break.
 */
describe("EventRouter canonical run attribution (CYR-72)", () => {
	let store: RouterStore;

	beforeEach(() => {
		store = new RouterStore(":memory:");
	});

	async function routedRun(overrides?: { logger?: TestLogger }) {
		const logger = overrides?.logger ?? testLogger();
		const deviceId = enroll(store, ALICE.email, {
			name: ALICE.name,
			linearId: ALICE.id,
		});
		const { router, clock } = makeRouter(store, {
			logger,
			// The project is on no webhook at any nesting, so it takes a Linear
			// read. Supplied here because "the project column is populated" is
			// exactly the filter dimension CYR-72 adds.
			fetchRoutingContext: async () => ({
				linearProjectId: "proj-7",
				linearProjectName: "Fleet observability",
			}),
		});
		const event = createdEvent({
			sessionId: "sess-1",
			issueId: "ISS-1",
			identifier: "CYR-72",
			creator: ALICE,
		}) as unknown as { agentSession: { issue: Record<string, unknown> } };
		event.agentSession.issue.team = { id: "team-3", name: "Cyrus" };
		event.agentSession.issue.teamId = "team-3";
		await router.route(event as unknown as AgentEvent);
		return { router, logger, deviceId, clock };
	}

	/** The identity columns a query joins on, from one emitted event. */
	function joinKey(attributes: Record<string, unknown>) {
		return {
			workspace_id: attributes.workspace_id,
			owner_id: attributes.owner_id,
			owner_name: attributes.owner_name,
			team_id: attributes.team_id,
			team_name: attributes.team_name,
			issue_key: attributes.issue_key,
			run_id: attributes.run_id,
			session_id: attributes.session_id,
			device_id: attributes.device_id,
			source: attributes.source,
		};
	}

	it("stamps the full canonical set on the route that creates the run", async () => {
		const { logger, deviceId } = await routedRun();

		const [routed] = eventsNamed(logger, "run.routed");
		expect(routed).toMatchObject({
			workspace_id: "ws-1",
			owner_id: "1",
			owner_name: ALICE.name,
			team_id: "team-3",
			team_name: "Cyrus",
			issue_key: "CYR-72",
			session_id: "sess-1",
			device_id: deviceId,
			source: "router",
		});
		expect(routed?.run_id).toEqual(expect.any(String));
		// Not yet enriched. `run.routed` records what was known WHEN THE ROUTE
		// HAPPENED — waiting for the Linear round trip would make it a different
		// fact — so the project columns are honestly null here and populated on
		// every later event about the same run.
		expect(routed?.project_id).toBeNull();
	});

	it("keeps the join key identical from route to terminal", async () => {
		const { router, logger, deviceId } = await routedRun();

		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "complete",
			runner: "claude",
			model: "claude-opus-5",
		});

		const [routed] = eventsNamed(logger, "run.routed");
		const [finished] = eventsNamed(logger, "run.finished");
		expect(routed).toBeDefined();
		expect(finished).toBeDefined();
		expect(joinKey(finished ?? {})).toEqual(joinKey(routed ?? {}));
		expect(finished).toMatchObject({
			terminal_state: "complete",
			// The terminal frame is the only point an ordinary run reports these,
			// which is why the event is emitted after the write rather than before.
			runner: "claude",
			model: "claude-opus-5",
		});
	});

	it("carries the enriched project onto every event after the enrichment lands", async () => {
		const { router, logger, deviceId } = await routedRun();
		// The enrichment is fire-and-forget off the routing path; let it settle.
		await new Promise((resolve) => setImmediate(resolve));

		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "complete",
		});

		expect(eventsNamed(logger, "run.finished")[0]).toMatchObject({
			project_id: "proj-7",
			project_name: "Fleet observability",
		});
	});

	it("distinguishes a run the router ended itself from one that reported terminal", async () => {
		// Both end the run. Only one of them means the outcome was never observed,
		// and folding them into a single event would bury every silently-lost run
		// inside the healthy series.
		const { router, logger, deviceId } = await routedRun();
		// The device has processed everything queued for it, which is what makes
		// its declared-session list authoritative enough to reconcile against.
		store.ackEvent(deviceId, 1);

		await router.reconcileDeviceLocks(deviceId, []);

		const [routed] = eventsNamed(logger, "run.routed");
		const [unknown] = eventsNamed(logger, "run.unknown");
		expect(eventsNamed(logger, "run.finished")).toHaveLength(0);
		expect(unknown).toMatchObject({
			reason: "lock_reconciled",
			reporting_device_id: deviceId,
		});
		expect(joinKey(unknown ?? {})).toEqual(joinKey(routed ?? {}));
	});

	it("reports a stale-affinity reclaim under the same run", async () => {
		const { router, logger, deviceId } = await routedRun();

		// `session_affinity.established_ms` is stamped from the store's own wall
		// clock, not the router's injectable one, so the reconcile window has to
		// be measured against `Date.now()` rather than `clock.value`.
		router.reconcileDeviceAffinity(deviceId, [], Date.now() + 600_001);

		expect(eventsNamed(logger, "run.unknown")[0]).toMatchObject({
			reason: "affinity_reconciled",
			session_id: "sess-1",
			run_id: expect.any(String),
		});
	});

	it("attributes an ownership refusal to the run whose activity was refused", async () => {
		// This refusal IS user-visible data loss (NOR-405). Without attribution it
		// can only be found by someone who already knows the session id — not by
		// the per-workspace or per-issue query anyone would actually reach for.
		const { router, logger, deviceId } = await routedRun();
		const mallory = deviceId + 1;

		router.handleSessionState(mallory, {
			type: "session_state",
			id: "ss-forged",
			sessionId: "sess-1",
			state: "complete",
		});

		expect(eventsNamed(logger, "session.ownership_refused")[0]).toMatchObject({
			reason: "terminal_not_owned",
			workspace_id: "ws-1",
			issue_key: "CYR-72",
			session_id: "sess-1",
			run_id: expect.any(String),
			// The REFUSED device, not the run's owner — the point of the event is
			// which device made a claim it did not hold.
			device_id: mallory,
			owner_device_id: deviceId,
		});
	});

	it("emits every canonical key on every run event, so isnull() filters work", async () => {
		const { router, logger, deviceId } = await routedRun();
		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "complete",
		});

		const emitted = [
			...eventsNamed(logger, "run.routed"),
			...eventsNamed(logger, "run.finished"),
		];
		expect(emitted).toHaveLength(2);
		for (const attributes of emitted) {
			for (const key of CANONICAL_RUN_ATTRIBUTE_KEYS) {
				expect(attributes).toHaveProperty(key.slice("cyrus.".length));
			}
		}
	});
});

/**
 * Review follow-up (CYR-72): a lifecycle event must describe a real transition.
 *
 * Both `finishAgentRun` and `markAgentRunUnknown` return early on a run that is
 * already terminal, and both of those are ROUTINE rather than exceptional —
 * `routePrompted` re-establishes affinity for an already-terminal session
 * (PAR-146), and `RouterServer` applies a `session_state` frame before acking
 * it, so a device replays frames the router already applied. Emitting anyway
 * would file a healthy completed run under "nobody saw the outcome", which is
 * exactly the misdiagnosis the finished/unknown split exists to prevent.
 */
describe("EventRouter run lifecycle events describe real transitions", () => {
	let store: RouterStore;

	beforeEach(() => {
		store = new RouterStore(":memory:");
	});

	async function routedAndFinished() {
		const logger = testLogger();
		const deviceId = enroll(store, ALICE.email, {
			name: ALICE.name,
			linearId: ALICE.id,
		});
		const { router } = makeRouter(store, { logger });
		await router.route(
			createdEvent({
				sessionId: "sess-1",
				issueId: "ISS-1",
				identifier: "CYR-72",
				creator: ALICE,
			}),
		);
		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f1",
			sessionId: "sess-1",
			state: "complete",
		});
		return { router, logger, deviceId };
	}

	it("emits nothing when a device replays a terminal frame it already sent", async () => {
		const { router, logger, deviceId } = await routedAndFinished();
		expect(eventsNamed(logger, "run.finished")).toHaveLength(1);

		router.handleSessionState(deviceId, {
			type: "session_state",
			id: "f2",
			sessionId: "sess-1",
			state: "complete",
		});

		expect(eventsNamed(logger, "run.finished")).toHaveLength(1);
	});

	it("does not report an already-finished run as one nobody observed", async () => {
		// The PAR-146 shape: affinity survives a terminal session, so a later
		// reclaim of that row reaches markAgentRunUnknown with nothing left to end.
		const { router, logger, deviceId } = await routedAndFinished();
		store.setSessionAffinity("sess-1", deviceId, undefined, 1_000);

		router.reconcileDeviceAffinity(deviceId, [], 1_000 + 600_001);

		expect(eventsNamed(logger, "run.unknown")).toHaveLength(0);
		expect(eventsNamed(logger, "run.finished")).toHaveLength(1);
	});

	it("still reports a genuine reclaim of a live run", async () => {
		// The guard must not suppress the case the event exists for.
		const logger = testLogger();
		const deviceId = enroll(store, ALICE.email, {
			name: ALICE.name,
			linearId: ALICE.id,
		});
		const { router } = makeRouter(store, { logger });
		await router.route(
			createdEvent({ sessionId: "sess-1", issueId: "ISS-1", creator: ALICE }),
		);

		router.reconcileDeviceAffinity(deviceId, [], Date.now() + 600_001);

		expect(eventsNamed(logger, "run.unknown")).toHaveLength(1);
	});
});
