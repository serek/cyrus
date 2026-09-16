import { LinearClient } from "@linear/sdk";
import { ClaudeRunner } from "cyrus-claude-runner";
import { LinearEventTransport } from "cyrus-linear-event-transport";
import { createCyrusToolsServer } from "cyrus-mcp-tools";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AgentSessionManager } from "../src/AgentSessionManager.js";
import { EdgeWorker } from "../src/EdgeWorker.js";
import { SharedApplicationServer } from "../src/SharedApplicationServer.js";
import type { EdgeWorkerConfig, RepositoryConfig } from "../src/types.js";
import { TEST_CYRUS_HOME } from "./test-dirs.js";

vi.mock("fs/promises");
vi.mock("cyrus-claude-runner");
vi.mock("cyrus-codex-runner");
vi.mock("cyrus-linear-event-transport");
vi.mock("cyrus-mcp-tools");
vi.mock("@linear/sdk");
vi.mock("../src/SharedApplicationServer.js");
vi.mock("../src/AgentSessionManager.js");
vi.mock("cyrus-core", async (importOriginal) => {
	const actual = (await importOriginal()) as object;
	return {
		...actual,
		PersistenceManager: vi.fn().mockImplementation(function () {
			return {
				loadEdgeWorkerState: vi.fn().mockResolvedValue(null),
				saveEdgeWorkerState: vi.fn().mockResolvedValue(undefined),
			};
		}),
	};
});

describe("EdgeWorker - OpenCode prompted-session routing", () => {
	const repository: RepositoryConfig = {
		id: "repo-1",
		name: "Repository",
		repositoryPath: "/test/repository",
		workspaceBaseDir: "/test/workspaces",
		baseBranch: "main",
		linearWorkspaceId: "workspace-1",
		isActive: true,
		allowedTools: ["Read"],
		labelPrompts: {},
	};

	let edgeWorker: EdgeWorker;
	let agentSessionManager: Record<string, ReturnType<typeof vi.fn>>;

	beforeEach(() => {
		vi.clearAllMocks();
		vi.spyOn(console, "log").mockImplementation(() => {});
		vi.spyOn(console, "error").mockImplementation(() => {});

		vi.mocked(createCyrusToolsServer).mockReturnValue({ server: {} } as any);
		vi.mocked(ClaudeRunner).mockImplementation(function () {
			return {} as any;
		});
		vi.mocked(LinearClient).mockImplementation(function () {
			return { users: { me: vi.fn() } } as any;
		});
		vi.mocked(LinearEventTransport).mockImplementation(function () {
			return { on: vi.fn(), register: vi.fn() } as any;
		});
		vi.mocked(SharedApplicationServer).mockImplementation(function () {
			return { registerOAuthCallbackHandler: vi.fn() } as any;
		});

		agentSessionManager = {
			getSession: vi.fn(),
			clearStopRequest: vi.fn(),
			reportOpenCodeFollowUpQueued: vi.fn().mockResolvedValue(undefined),
			on: vi.fn(),
		};
		vi.mocked(AgentSessionManager).mockImplementation(function () {
			return agentSessionManager as any;
		});

		const config: EdgeWorkerConfig = {
			proxyUrl: "http://localhost:3000",
			cyrusHome: TEST_CYRUS_HOME,
			repositories: [repository],
			linearWorkspaces: { "workspace-1": { linearToken: "token" } },
		};
		edgeWorker = new EdgeWorker(config);
	});

	afterEach(() => vi.restoreAllMocks());

	function openCodeSession(runner: Record<string, unknown>) {
		return {
			id: "agent-session-1",
			opencodeSessionId: "opencode-session-1",
			issueId: "issue-1",
			issueContext: { issueId: "issue-1" },
			workspace: { path: "/test/workspaces/issue-1" },
			agentRunner: runner,
		} as any;
	}

	it("preserves an active OpenCode runner when streaming input is unavailable", async () => {
		let onComplete: (() => void) | undefined;
		const runner = {
			isRunning: vi.fn().mockReturnValue(true),
			supportsStreamingInput: false,
			stop: vi.fn(),
			once: vi.fn((_event: string, listener: () => void) => {
				onComplete = listener;
			}),
		};
		const session = openCodeSession(runner);
		agentSessionManager.getSession.mockReturnValue(session);
		const resume = vi
			.spyOn(edgeWorker, "resumeAgentSession")
			.mockResolvedValue(undefined);

		await (edgeWorker as any).handlePromptWithStreamingCheck(
			session,
			repository,
			session.id,
			agentSessionManager,
			"Please continue",
			"",
			false,
			[],
			"prompted webhook",
			"workspace-1",
		);

		expect(runner.stop).not.toHaveBeenCalled();
		expect(resume).not.toHaveBeenCalled();
		expect(runner.once).toHaveBeenCalledWith("complete", expect.any(Function));
		expect(onComplete).toBeTypeOf("function");
	});

	it("resumes a dead OpenCode runner instead of deferring the prompt", async () => {
		const runner = {
			isRunning: vi.fn().mockReturnValue(false),
			supportsStreamingInput: false,
			stop: vi.fn(),
		};
		const session = openCodeSession(runner);
		const resume = vi
			.spyOn(edgeWorker, "resumeAgentSession")
			.mockResolvedValue(undefined);

		await (edgeWorker as any).handlePromptWithStreamingCheck(
			session,
			repository,
			session.id,
			agentSessionManager,
			"Resume this work",
			"",
			false,
			[],
			"prompted webhook",
			"workspace-1",
		);

		expect(resume).toHaveBeenCalledOnce();
		expect(runner.stop).not.toHaveBeenCalled();
	});

	it("delivers queued prompts after the active OpenCode turn completes", async () => {
		let running = true;
		let onComplete: (() => void) | undefined;
		const runner = {
			isRunning: vi.fn(() => running),
			supportsStreamingInput: false,
			stop: vi.fn(),
			once: vi.fn((_event: string, listener: () => void) => {
				onComplete = listener;
			}),
		};
		const session = openCodeSession(runner);
		agentSessionManager.getSession.mockReturnValue(session);
		const resume = vi
			.spyOn(edgeWorker, "resumeAgentSession")
			.mockResolvedValue(undefined);

		await (edgeWorker as any).handlePromptWithStreamingCheck(
			session,
			repository,
			session.id,
			agentSessionManager,
			"Queued follow-up",
			"attachment manifest",
			false,
			[],
			"prompted webhook",
			"workspace-1",
		);

		running = false;
		onComplete?.();

		await vi.waitFor(() => {
			expect(resume).toHaveBeenCalledWith(
				session,
				repository,
				session.id,
				agentSessionManager,
				"Queued follow-up",
				"attachment manifest",
				false,
				[],
				"workspace-1",
				undefined,
				undefined,
				undefined,
			);
		});
	});
});
