using System;
using System.Text.RegularExpressions;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;

namespace VsAgentic.UI.ViewModels.Banners;

/// <summary>
/// Shown when the CLI reports that it is discarding this workspace's
/// <c>permissions.allow</c> entries because the folder has never been trusted.
///
/// Nothing is broken — the session runs — but the user is prompted for tools
/// they already allowed, and the CLI says so only on stderr. The extension
/// always runs the CLI headless, which is precisely the mode that never shows
/// the trust dialog, so without this the workspace can never become trusted
/// from inside the IDE.
/// </summary>
public partial class TrustBannerViewModel : ObservableObject, IBannerViewModel
{
    private readonly Action _onTrustClicked;
    private readonly Action _onDismissed;

    public string Title => "This workspace isn't trusted by Claude Code";
    public string DetailMessage { get; }
    public string SubText =>
        "A console window will open in this folder. Accept the trust prompt there, then close it — your next " +
        "message picks up the change. Until then, tools you have already allow-listed will keep prompting.";

    public TrustBannerViewModel(string? cliMessage, Action onTrustClicked, Action onDismissed)
    {
        _onTrustClicked = onTrustClicked;
        _onDismissed = onDismissed;
        DetailMessage = Summarize(cliMessage);
    }

    /// <summary>
    /// Keeps the part of the CLI's warning that says what is being lost and
    /// drops its manual fix-it instructions, which the buttons below replace.
    /// </summary>
    private static string Summarize(string? cliMessage)
    {
        const string fallback =
            "Claude Code is ignoring the permission allow-list in this workspace's settings.";

        if (string.IsNullOrWhiteSpace(cliMessage)) return fallback;

        // "Ignoring 23 permissions.allow entries from <files>: this workspace
        // has not been trusted. Run Claude Code interactively here once ..."
        var match = Regex.Match(
            cliMessage,
            @"Ignoring\s+(\d+)\s+permissions\.allow\s+entries",
            RegexOptions.IgnoreCase);

        return match.Success
            ? $"Claude Code is ignoring {match.Groups[1].Value} permission allow-list entries from this "
              + "workspace's settings because the folder has not been trusted."
            : fallback;
    }

    [RelayCommand]
    private void Trust() => _onTrustClicked();

    [RelayCommand]
    private void Dismiss() => _onDismissed();
}
