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

describe("EdgeWorker - Linear issue-owner comment routing", () => {
	const repository: RepositoryConfig = {
		id: "repo-1",
		name: "Repository",
		repositoryPath: "/test/repository",
		workspaceBaseDir: "/test/workspaces",
		baseBranch: "main",
		linearWorkspaceId: "workspace-1",
		isActive: true,
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
			getActiveSessionsByIssueId: vi.fn(),
			on: vi.fn(),
		};
		vi.mocked(AgentSessionManager).mockImplementation(function () {
			return agentSessionManager as any;
		});

		edgeWorker = new EdgeWorker({
			proxyUrl: "http://localhost:3000",
			cyrusHome: TEST_CYRUS_HOME,
			repositories: [repository],
			linearWorkspaces: { "workspace-1": { linearToken: "token" } },
		} satisfies EdgeWorkerConfig);
	});

	afterEach(() => vi.restoreAllMocks());

	function issueCommentWebhook(overrides: Record<string, unknown> = {}) {
		return {
			type: "AppUserNotification",
			action: "issueNewComment",
			createdAt: "2026-09-16T00:00:00.000Z",
			organizationId: "workspace-1",
			notification: {
				actorId: "owner-1",
				issue: { id: "issue-1", identifier: "ENG-1", title: "Issue" },
				comment: {
					id: "comment-1",
					body: "Please continue",
					userId: "owner-1",
				},
			},
			...overrides,
		};
	}

	it("routes an active session owner's ordinary comment through prompted routing", async () => {
		agentSessionManager.getActiveSessionsByIssueId.mockReturnValue([
			{ id: "session-1", creator: { id: "owner-1", name: "Owner" } },
		]);
		const handlePrompt = vi
			.spyOn(edgeWorker as any, "handleUserPromptedAgentActivity")
			.mockResolvedValue(undefined);

		await (edgeWorker as any).handleWebhook(issueCommentWebhook(), [
			repository,
		]);

		expect(agentSessionManager.getActiveSessionsByIssueId).toHaveBeenCalledWith(
			"issue-1",
		);
		expect(handlePrompt).toHaveBeenCalledWith(
			expect.objectContaining({
				type: "AgentSessionEvent",
				action: "prompted",
				agentSession: expect.objectContaining({ id: "session-1" }),
				agentActivity: {
					content: { body: "Please continue" },
					sourceCommentId: "comment-1",
				},
			}),
		);
	});

	it("does not route a comment from someone other than the active session owner", async () => {
		agentSessionManager.getActiveSessionsByIssueId.mockReturnValue([
			{ id: "session-1", creator: { id: "owner-1" } },
		]);
		const handlePrompt = vi.spyOn(
			edgeWorker as any,
			"handleUserPromptedAgentActivity",
		);

		await (edgeWorker as any).handleWebhook(
			issueCommentWebhook({
				notification: {
					actorId: "other-user",
					issue: { id: "issue-1", identifier: "ENG-1", title: "Issue" },
					comment: {
						id: "comment-1",
						body: "Please continue",
						userId: "other-user",
					},
				},
			}),
			[repository],
		);

		expect(handlePrompt).not.toHaveBeenCalled();
	});

	it("does not choose between multiple owner sessions on the same issue", async () => {
		agentSessionManager.getActiveSessionsByIssueId.mockReturnValue([
			{ id: "session-1", creator: { id: "owner-1" } },
			{ id: "session-2", creator: { id: "owner-1" } },
		]);
		const handlePrompt = vi.spyOn(
			edgeWorker as any,
			"handleUserPromptedAgentActivity",
		);

		await (edgeWorker as any).handleWebhook(issueCommentWebhook(), [
			repository,
		]);

		expect(handlePrompt).not.toHaveBeenCalled();
	});
});
