using Microsoft.Data.Sqlite;
using WhatsAppBridge.Api.Models;

namespace WhatsAppBridge.Api.Data;

public sealed class MessageStore
{
    private readonly string _connectionString;

    public MessageStore(IConfiguration configuration)
    {
        _connectionString =
            configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException(
                "Database connection string not configured."
            );
    }

    public bool Save(MessageRecord message)
    {
        using var connection = new SqliteConnection(_connectionString);

        connection.Open();

        using var command = connection.CreateCommand();

        command.CommandText =
            """
            INSERT INTO messages (
                redis_event_id,
                whatsapp_message_id,
                chat_jid,
                sender_jid,
                text,
                from_me,
                message_type,
                created_at,
                persisted_at
            )
            VALUES (
                $redisEventId,
                $whatsappMessageId,
                $chatJid,
                $senderJid,
                $text,
                $fromMe,
                $messageType,
                $createdAt,
                $persistedAt
            )
            ON CONFLICT(redis_event_id) DO NOTHING;
            """;

        command.Parameters.AddWithValue(
            "$redisEventId",
            message.RedisEventId
        );

        command.Parameters.AddWithValue(
            "$whatsappMessageId",
            message.WhatsAppMessageId
        );

        command.Parameters.AddWithValue(
            "$chatJid",
            message.ChatJid
        );

        command.Parameters.AddWithValue(
            "$senderJid",
            message.SenderJid
        );

        command.Parameters.AddWithValue(
            "$text",
            message.Text
        );

        command.Parameters.AddWithValue(
            "$fromMe",
            message.FromMe ? 1 : 0
        );

        command.Parameters.AddWithValue(
            "$messageType",
            message.MessageType
        );

        command.Parameters.AddWithValue(
            "$createdAt",
            message.CreatedAt.ToString("O")
        );

        command.Parameters.AddWithValue(
            "$persistedAt",
            message.PersistedAt.ToString("O")
        );

        var affectedRows = command.ExecuteNonQuery();

        return affectedRows > 0;
    }

    public IReadOnlyList<MessageRecord> GetMessages(int limit = 100)
    {
        var messages = new List<MessageRecord>();

        using var connection =
            new SqliteConnection(_connectionString);

        connection.Open();

        using var command = connection.CreateCommand();

        command.CommandText =
            """
            SELECT
                id,
                redis_event_id,
                whatsapp_message_id,
                chat_jid,
                sender_jid,
                text,
                from_me,
                message_type,
                created_at,
                persisted_at
            FROM messages
            ORDER BY id DESC
            LIMIT $limit;
            """;

        command.Parameters.AddWithValue(
            "$limit",
            limit
        );

        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            messages.Add(
                new MessageRecord
                {
                    Id = reader.GetInt64(0),

                    RedisEventId =
                        reader.GetString(1),

                    WhatsAppMessageId =
                        reader.IsDBNull(2)
                            ? string.Empty
                            : reader.GetString(2),

                    ChatJid =
                        reader.IsDBNull(3)
                            ? string.Empty
                            : reader.GetString(3),

                    SenderJid =
                        reader.IsDBNull(4)
                            ? string.Empty
                            : reader.GetString(4),

                    Text =
                        reader.IsDBNull(5)
                            ? string.Empty
                            : reader.GetString(5),

                    FromMe =
                        reader.GetInt64(6) == 1,

                    MessageType =
                        reader.IsDBNull(7)
                            ? string.Empty
                            : reader.GetString(7),

                    CreatedAt =
                        DateTimeOffset.Parse(
                            reader.GetString(8)
                        ),

                    PersistedAt =
                        DateTimeOffset.Parse(
                            reader.GetString(9)
                        )
                }
            );
        }

        return messages;
    }
}
