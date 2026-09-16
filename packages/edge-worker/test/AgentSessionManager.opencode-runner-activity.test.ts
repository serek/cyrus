import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { OpenCodeRunner } from "cyrus-opencode-runner";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AgentSessionManager } from "../src/AgentSessionManager";
import type { IActivitySink } from "../src/sinks/IActivitySink";

function makeTempDir(): string {
	return mkdtempSync(join(tmpdir(), "cyrus-opencode-activity-"));
}

function fixtureLines(): string {
	const url = new URL(
		"../../opencode-runner/test/fixtures/opencode-run-activity.jsonl",
		import.meta.url,
	);
	return readFileSync(url, "utf8");
}

function writeFakeOpenCode(dir: string): string {
	const script = join(dir, "fake-opencode.mjs");
	writeFileSync(
		script,
		`#!/usr/bin/env node
process.stdin.resume();
process.stdout.write(${JSON.stringify(fixtureLines())});
`,
		{ mode: 0o755 },
	);
	return script;
}

describe("AgentSessionManager - OpenCode activity mapping", () => {
	let manager: AgentSessionManager;
	let mockActivitySink: IActivitySink;
	let postActivitySpy: ReturnType<typeof vi.fn>;
	const sessionId = "test-session-opencode";
	const issueId = "issue-opencode";

	beforeEach(() => {
		mockActivitySink = {
			id: "test-workspace",
			postActivity: vi.fn().mockResolvedValue({ activityId: "activity-123" }),
			createAgentSession: vi.fn().mockResolvedValue("session-123"),
		};

		postActivitySpy = vi.spyOn(mockActivitySink, "postActivity");
		manager = new AgentSessionManager();

		manager.createCyrusAgentSession(
			sessionId,
			issueId,
			{
				id: issueId,
				identifier: "TEST-200",
				title: "OpenCode activity test",
				description: "",
				branchName: "test-branch",
			},
			{
				path: "/tmp/cyrus-opencode-activity",
				isGitWorktree: false,
			},
		);
		manager.setActivitySink(sessionId, mockActivitySink);
	});

	it("creates Linear action and response entries for OpenCode text and tool events", async () => {
		const dir = makeTempDir();
		const runner = new OpenCodeRunner({
			openCodePath: writeFakeOpenCode(dir),
			workingDirectory: dir,
			cyrusHome: dir,
			opencodeGlobalConfig: {
				model: "anthropic/claude-sonnet-4.5",
			},
		});
		manager.addAgentRunner(sessionId, runner);

		await runner.start("Inspect and update src/index.ts");
		for (const message of runner.getMessages()) {
			await manager.handleClaudeMessage(sessionId, message);
		}

		const calls = postActivitySpy.mock.calls;
		expect(calls.length).toBeGreaterThanOrEqual(6);

		expect(
			calls.some(
				(call: any[]) =>
					call[1]?.type === "thought" &&
					call[1]?.body === "Using model: opencode/anthropic/claude-sonnet-4.5",
			),
		).toBe(true);

		const readActionWithResult = calls.find(
			(call: any[]) =>
				call[1]?.type === "action" &&
				call[1]?.action === "Read" &&
				call[1]?.parameter === "src/index.ts" &&
				typeof call[1]?.result === "string",
		);
		expect(readActionWithResult).toBeDefined();
		expect(readActionWithResult![1]?.result).toContain("export const value");

		const editActionWithResult = calls.find(
			(call: any[]) =>
				call[1]?.type === "action" &&
				call[1]?.action === "Edit" &&
				call[1]?.parameter === "src/index.ts" &&
				typeof call[1]?.result === "string",
		);
		expect(editActionWithResult).toBeDefined();
		expect(editActionWithResult![1]?.result).toContain(
			"export const value = 2",
		);

		const todoThought = calls.find(
			(call: any[]) =>
				call[1]?.type === "thought" &&
				typeof call[1]?.body === "string" &&
				call[1]?.body.includes(
					"- [x] Explore cyrus-hosted /settings/tools page and current platform selector",
				) &&
				call[1]?.body.includes(
					"- [ ] Add toolsets to cyrus-core EdgeConfig schema + regenerate JSON schemas (in progress)",
				) &&
				call[1]?.body.includes(
					"- [ ] Wire toolsets through cyrus ConfigManager and ToolPermissionResolver (pending)",
				),
		);
		expect(todoThought).toBeDefined();
		expect(
			calls.some(
				(call: any[]) =>
					call[1]?.type === "action" &&
					call[1]?.action === "todowrite" &&
					typeof call[1]?.parameter === "string" &&
					call[1]?.parameter.includes('"todos"'),
			),
		).toBe(false);

		const abortedReadAction = calls.find(
			(call: any[]) =>
				call[1]?.type === "action" &&
				call[1]?.action === "Read" &&
				call[1]?.parameter === "src/missing.ts" &&
				typeof call[1]?.result === "string",
		);
		expect(abortedReadAction).toBeDefined();
		expect(abortedReadAction![1]?.result).toContain("tool call aborted");

		const abortedGenericAction = calls.find(
			(call: any[]) =>
				call[1]?.type === "action" &&
				call[1]?.action === "OpenCode tool call" &&
				call[1]?.parameter === "pnpm test" &&
				typeof call[1]?.result === "string",
		);
		expect(abortedGenericAction).toBeDefined();
		expect(abortedGenericAction![1]?.result).toContain("tool call aborted");
		expect(
			calls.some(
				(call: any[]) =>
					call[1]?.type === "action" && call[1]?.action === "unknown",
			),
		).toBe(false);

		const finalResponse = calls.find(
			(call: any[]) =>
				call[1]?.type === "response" &&
				typeof call[1]?.body === "string" &&
				call[1]?.body.includes("Updated src/index.ts"),
		);
		expect(finalResponse).toBeDefined();
	});

	it("uses explicit OpenCode provider/model selection in the model notification", async () => {
		const dir = makeTempDir();
		const runner = new OpenCodeRunner({
			openCodePath: writeFakeOpenCode(dir),
			workingDirectory: dir,
			cyrusHome: dir,
			model: "openai/gpt-5.5",
		});
		manager.addAgentRunner(sessionId, runner);

		await runner.start("Inspect and update src/index.ts");
		for (const message of runner.getMessages()) {
			await manager.handleClaudeMessage(sessionId, message);
		}

		expect(
			postActivitySpy.mock.calls.some(
				(call: any[]) =>
					call[1]?.type === "thought" &&
					call[1]?.body === "Using model: opencode/openai/gpt-5.5",
			),
		).toBe(true);
	});

	it.each([2, 3, 5])(
		"keeps %i concurrent OpenCode sessions and Linear activities isolated",
		async (sessionCount) => {
			const dir = makeTempDir();
			const script = join(dir, "fake-concurrent-opencode.mjs");
			writeFileSync(
				script,
				`#!/usr/bin/env node
import { readFileSync } from "node:fs";
const prompt = readFileSync(0, "utf8");
const lane = prompt.match(/lane-(\\d+)/)?.[1] ?? "unknown";
process.stdout.write(JSON.stringify({ type: "step_start", sessionID: "oc_" + lane }) + "\\n");
process.stdout.write(JSON.stringify({ type: "text", part: { text: lane + " progress" } }) + "\\n");
process.stdout.write(JSON.stringify({ type: "text", part: { text: lane + " final" } }) + "\\n");
process.stdout.write(JSON.stringify({ type: "step_finish", result: lane + " complete" }) + "\\n");
`,
				{ mode: 0o755 },
			);

			const sinkFor = (id: string) => ({
				id,
				postActivity: vi
					.fn()
					.mockResolvedValue({ activityId: `${id}-activity` }),
				createAgentSession: vi.fn().mockResolvedValue(`${id}-external`),
			});
			const sessions = Array.from({ length: sessionCount }, (_, index) => {
				const lane = `lane-${index + 1}`;
				return {
					lane,
					sessionId: `test-session-${lane}`,
					issueId: `issue-${lane}`,
					sink: sinkFor(lane),
				};
			});

			for (const { lane, sessionId, issueId, sink } of sessions) {
				manager.createCyrusAgentSession(
					sessionId,
					issueId,
					{
						id: issueId,
						identifier: lane,
						title: `${sessionId} activity test`,
						description: "",
						branchName: `${sessionId}-branch`,
					},
					{ path: dir, isGitWorktree: false },
				);
				manager.setActivitySink(sessionId, sink);
			}

			const runners = await Promise.all(
				sessions.map(async ({ lane, sessionId }) => {
					const runner = new OpenCodeRunner({
						openCodePath: script,
						workingDirectory: dir,
						cyrusHome: dir,
					});
					manager.addAgentRunner(sessionId, runner);
					await runner.start(`Handle ${lane} task`);
					return { lane, sessionId, runner };
				}),
			);
			await Promise.all(
				runners.map(async ({ sessionId, runner }) => {
					// Sessions still run concurrently, while each session's timeline
					// records runner output before its compact health marker.
					for (const message of runner.getMessages()) {
						await manager.handleClaudeMessage(sessionId, message);
					}
					await manager.reportRunnerHealth(sessionId, { type: "alive" });
					await manager.reportOpenCodeFollowUpQueued(sessionId);
				}),
			);
			await Promise.all(
				runners.map(({ sessionId, runner }) =>
					manager.completeSession(sessionId, {
						type: "result",
						subtype: "success",
						result: `completed ${sessionId}`,
						session_id: runner.getMessages()[0].session_id,
					} as any),
				),
			);

			for (const { lane, sessionId, sink } of sessions) {
				const bodies = sink.postActivity.mock.calls.map(([, activity]) =>
					JSON.stringify(activity),
				);
				expect(
					sink.postActivity.mock.calls.every(([id]) => id === sessionId),
				).toBe(true);
				expect(manager.getSession(sessionId)?.issueId).toBe(`issue-${lane}`);
				expect(manager.getSession(sessionId)?.opencodeSessionId).toBe(
					`oc_${lane.replace("lane-", "")}`,
				);
				expect(
					bodies.some((body) =>
						body.includes(`${lane.replace("lane-", "")} progress`),
					),
				).toBe(true);
				expect(bodies.some((body) => body.includes("Runner alive."))).toBe(
					true,
				);
				expect(
					bodies.some((body) => body.includes("A follow-up is queued")),
				).toBe(true);
				expect(
					bodies.some((body) => body.includes(`completed ${sessionId}`)),
				).toBe(true);
			}
		},
	);

	it("posts bounded, safe OpenCode lifecycle visibility through public seams", async () => {
		const runner = new OpenCodeRunner({
			openCodePath: "/bin/true",
			workingDirectory: makeTempDir(),
			cyrusHome: makeTempDir(),
		});
		manager.addAgentRunner(sessionId, runner);

		await manager.handleClaudeMessage(sessionId, {
			type: "system",
			subtype: "init",
			session_id: "oc-first",
			model: "opencode/test",
			tools: [],
		} as any);
		await manager.reportOpenCodeFollowUpQueued(sessionId);
		await manager.reportOpenCodeFollowUpQueued(sessionId);
		await manager.completeSession(sessionId, {
			type: "result",
			subtype: "success",
			duration_ms: 1,
			duration_api_ms: 1,
			is_error: false,
			num_turns: 1,
			result: "final response remains intact",
			total_cost_usd: 0,
			usage: {},
			modelUsage: {},
			permission_denials: [],
			uuid: "result-first",
			session_id: "oc-first",
		} as any);

		manager.addAgentRunner(sessionId, runner);
		await manager.handleClaudeMessage(sessionId, {
			type: "system",
			subtype: "init",
			session_id: "oc-resumed",
			model: "opencode/test",
			tools: [],
		} as any);
		await manager.failSession(
			sessionId,
			"secret prompt /private/path --token=abc",
		);

		const lifecycleBodies = postActivitySpy.mock.calls
			.map(([, activity]) => activity)
			.filter(
				(activity: any) =>
					typeof activity?.body === "string" &&
					(activity.body.startsWith("OpenCode") ||
						activity.body.startsWith("A follow-up is queued")),
			)
			.map((activity: any) => activity.body);

		expect(lifecycleBodies).toEqual([
			"OpenCode started work on this request.",
			"A follow-up is queued and will be handled after this OpenCode turn.",
			"OpenCode completed this turn.",
			"OpenCode resumed work on this request.",
			"OpenCode ended unexpectedly.",
		]);
		expect(lifecycleBodies.join("\n")).not.toContain("/private/path");
		expect(lifecycleBodies.join("\n")).not.toContain("token=abc");
		expect(
			postActivitySpy.mock.calls.some(
				([, activity]: any[]) =>
					activity?.type === "response" &&
					activity.body === "final response remains intact",
			),
		).toBe(true);
	});

	it("deduplicates repeated OpenCode progress milestones within a session", async () => {
		const runner = new OpenCodeRunner({
			openCodePath: "/bin/true",
			workingDirectory: makeTempDir(),
			cyrusHome: makeTempDir(),
		});
		manager.addAgentRunner(sessionId, runner);

		const toolUse = {
			type: "assistant",
			message: {
				content: [
					{
						type: "tool_use",
						id: "tool-read",
						name: "Read",
						input: { file_path: "README.md" },
					},
				],
			},
			session_id: "oc-dedup",
		} as any;

		await manager.handleClaudeMessage(sessionId, toolUse);
		await manager.handleClaudeMessage(sessionId, {
			...toolUse,
			message: {
				content: [
					{
						type: "tool_use",
						id: "tool-edit",
						name: "Edit",
						input: { file_path: "README.md" },
					},
				],
			},
		});
		await manager.handleClaudeMessage(sessionId, toolUse);

		expect(
			postActivitySpy.mock.calls.filter(
				([, activity]: any[]) =>
					activity?.type === "thought" &&
					activity.body === "OpenCode started a tool step.",
			).length,
		).toBe(2);
	});

	it("reports an intentional OpenCode stop without exposing operator input", async () => {
		const runner = new OpenCodeRunner({
			openCodePath: "/bin/true",
			workingDirectory: makeTempDir(),
			cyrusHome: makeTempDir(),
		});
		manager.addAgentRunner(sessionId, runner);

		await manager.abortSession(sessionId);

		expect(postActivitySpy).toHaveBeenCalledWith(
			sessionId,
			{ type: "error", body: "OpenCode was stopped by an operator." },
			{},
		);
		expect(
			postActivitySpy.mock.calls
				.map(([, activity]) => JSON.stringify(activity))
				.join("\n"),
		).not.toContain("/tmp/");
	});

	it("renders a compact OpenCode failure milestone after host-interruption recovery", async () => {
		const runner = new OpenCodeRunner({
			openCodePath: "/bin/true",
			workingDirectory: makeTempDir(),
			cyrusHome: makeTempDir(),
		});
		manager.addAgentRunner(sessionId, runner);
		await manager.handleClaudeMessage(sessionId, {
			type: "system",
			subtype: "init",
			session_id: "oc-interrupted",
			model: "opencode/test",
			tools: [],
		} as any);

		const interruptedSession = manager.getSession(sessionId);
		expect(interruptedSession).toBeDefined();
		interruptedSession!.agentRunner = undefined;
		await manager.reconcileInterruptedSessions();

		expect(
			postActivitySpy.mock.calls.filter(
				([, activity]: any[]) =>
					activity?.body === "OpenCode ended unexpectedly.",
			),
		).toHaveLength(1);
		expect(postActivitySpy).toHaveBeenCalledWith(
			sessionId,
			{
				type: "error",
				body: "Session interrupted — the agent host restarted before this session finished. Send a new message to resume it.",
			},
			{},
		);
	});
});
