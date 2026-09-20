using System;
using System.IO;
using Microsoft.Extensions.Logging;

namespace VsAgentic.Services.ClaudeCli;

/// <summary>
/// Locates the CLI's own transcript file for a session id.
///
/// The CLI writes one <c>&lt;session-id&gt;.jsonl</c> per session under
/// <c>&lt;config dir&gt;/projects/&lt;encoded working directory&gt;/</c> and prunes
/// old ones on its own schedule (the <c>cleanupPeriodDays</c> setting, 30 days by
/// default). A session id we persisted months ago can therefore name a
/// conversation the CLI no longer has, and passing that to <c>--resume</c> is
/// fatal: the CLI writes "No conversation found with session ID: ..." to stderr
/// and exits before reading a single byte of stdin.
///
/// Rather than reproduce the undocumented working-directory-to-folder encoding
/// ("E:\Lingos" -> "E--Lingos"), we search the projects tree by file name.
/// </summary>
public static class ClaudeSessionTranscript
{
    /// <summary>
    /// Returns the transcript path for <paramref name="sessionId"/>, or null when
    /// there isn't one (or we couldn't look).
    /// </summary>
    public static string? FindPath(string? sessionId, ILogger? logger = null)
    {
        if (!TryNormalize(sessionId, out var id)) return null;

        var root = ProjectsRoot();
        if (root is null || !Directory.Exists(root)) return null;

        try
        {
            foreach (var file in Directory.EnumerateFiles(root, id + ".jsonl", SearchOption.AllDirectories))
                return file;
        }
        catch (Exception ex)
        {
            logger?.LogDebug(ex, "[SessionTranscript] Could not search {Root} for session {SessionId}", root, id);
        }

        return null;
    }

    /// <summary>
    /// True only when we positively established that the CLI has no transcript for
    /// this session — the projects folder was readable and contained no matching
    /// file. Anything we cannot determine (no config dir, an unreadable tree, a
    /// malformed id) answers false.
    ///
    /// The asymmetry is deliberate: a wrong "missing" makes the caller drop
    /// <c>--resume</c> and silently strand a conversation the CLI could have
    /// restored, which is far worse than the failure this guards against.
    /// </summary>
    public static bool IsKnownMissing(string? sessionId, ILogger? logger = null)
    {
        if (!TryNormalize(sessionId, out var id)) return false;

        var root = ProjectsRoot();
        if (root is null || !Directory.Exists(root))
        {
            logger?.LogDebug("[SessionTranscript] No readable projects folder; cannot judge session {SessionId}", id);
            return false;
        }

        try
        {
            foreach (var _ in Directory.EnumerateFiles(root, id + ".jsonl", SearchOption.AllDirectories))
                return false;
            return true;
        }
        catch (Exception ex)
        {
            // An unreadable subdirectory aborts the enumeration part-way, which
            // tells us nothing about whether the file is there.
            logger?.LogDebug(ex, "[SessionTranscript] Could not search {Root} for session {SessionId}", root, id);
            return false;
        }
    }

    private static bool TryNormalize(string? sessionId, out string id)
    {
        id = (sessionId ?? "").Trim();
        if (id.Length == 0) return false;
        // The id becomes a search pattern below, so anything that isn't a plain
        // file name is not ours to interpret.
        return id.IndexOfAny(Path.GetInvalidFileNameChars()) < 0;
    }

    /// <summary>
    /// The CLI's projects folder. <c>CLAUDE_CONFIG_DIR</c> relocates the whole
    /// config directory, so honour it before falling back to <c>~/.claude</c>.
    /// </summary>
    private static string? ProjectsRoot()
    {
        var configDir = Environment.GetEnvironmentVariable("CLAUDE_CONFIG_DIR");
        if (string.IsNullOrWhiteSpace(configDir))
        {
            var userProfile = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            if (string.IsNullOrEmpty(userProfile)) return null;
            configDir = Path.Combine(userProfile, ".claude");
        }

        try { return Path.Combine(configDir!.Trim(), "projects"); }
        catch (ArgumentException) { return null; }
    }
}
