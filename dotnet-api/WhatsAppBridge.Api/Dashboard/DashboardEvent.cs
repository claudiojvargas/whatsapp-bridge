namespace WhatsAppBridge.Api.Dashboard;

public sealed record DashboardEvent(
    string Id,
    string Type,
    DateTimeOffset ReceivedAt,
    IReadOnlyDictionary<string, string> Fields
);
