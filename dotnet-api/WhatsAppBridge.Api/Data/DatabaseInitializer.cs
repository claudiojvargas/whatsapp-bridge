using Microsoft.Data.Sqlite;

namespace WhatsAppBridge.Api.Data;

public sealed class DatabaseInitializer
{
    private readonly string _connectionString;

    public DatabaseInitializer(IConfiguration configuration)
    {
        _connectionString =
            configuration.GetConnectionString("DefaultConnection")
            ?? throw new InvalidOperationException(
                "Database connection string not configured."
            );
    }

    public void Initialize()
    {
        using var connection = new SqliteConnection(_connectionString);

        connection.Open();

        using var command = connection.CreateCommand();

        command.CommandText =
            """
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                redis_event_id TEXT NOT NULL UNIQUE,

                whatsapp_message_id TEXT,

                chat_jid TEXT,

                sender_jid TEXT,

                text TEXT,

                from_me INTEGER NOT NULL,

                message_type TEXT,

                created_at TEXT NOT NULL,

                persisted_at TEXT NOT NULL
            );
            """;

        command.ExecuteNonQuery();
    }
}
