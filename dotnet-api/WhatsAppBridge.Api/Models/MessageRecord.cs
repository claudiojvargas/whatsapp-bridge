namespace WhatsAppBridge.Api.Models;

public sealed class MessageRecord
{
    public long Id { get; set; }

    public string RedisEventId { get; set; } = string.Empty;

    public string WhatsAppMessageId { get; set; } = string.Empty;

    public string ChatJid { get; set; } = string.Empty;

    public string SenderJid { get; set; } = string.Empty;

    public string Text { get; set; } = string.Empty;

    public bool FromMe { get; set; }

    public string MessageType { get; set; } = string.Empty;

    public DateTimeOffset CreatedAt { get; set; }

    public DateTimeOffset PersistedAt { get; set; }
}
