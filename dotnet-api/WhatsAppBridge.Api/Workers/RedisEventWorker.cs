using StackExchange.Redis;
using WhatsAppBridge.Api.Dashboard;
using WhatsAppBridge.Api.Data;
using WhatsAppBridge.Api.Models;

namespace WhatsAppBridge.Api.Workers;

public sealed class RedisEventWorker : BackgroundService
{
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<RedisEventWorker> _logger;

    private readonly RedisKey _stream;
    private readonly RedisValue _group;
    private readonly RedisValue _consumer;

    private readonly MessageStore _messageStore;

    private readonly TimeSpan _pollInterval;

    private readonly DashboardEventStore _dashboard;

    public RedisEventWorker(
        IConnectionMultiplexer redis,
        IConfiguration configuration,
        ILogger<RedisEventWorker> logger,
	MessageStore messageStore,  
  DashboardEventStore dashboard)
    {
        _redis = redis;
        _logger = logger;
        _dashboard = dashboard;

        _stream =
            configuration["Redis:EventsStream"]
            ?? "agent:events";

        _group =
            configuration["Redis:ConsumerGroup"]
            ?? "dotnet-monitor";

        _consumer =
            configuration["Redis:ConsumerName"]
            ?? "api-1";

        var pollMilliseconds =
            configuration.GetValue<int?>(
                "Redis:PollMilliseconds"
            ) ?? 500;

	_messageStore = messageStore;

        _pollInterval =
            TimeSpan.FromMilliseconds(
                pollMilliseconds
            );
    }

    protected override async Task ExecuteAsync(
        CancellationToken stoppingToken)
    {
        var database = _redis.GetDatabase();

        var initialized = false;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                if (!initialized)
                {
                    await EnsureConsumerGroupAsync(
                        database
                    );

                    await ProcessPendingMessagesAsync(
                        database,
                        stoppingToken
                    );

                    initialized = true;

                    _logger.LogInformation(
                        "Redis event worker connected. Stream={Stream} Group={Group} Consumer={Consumer}",
                        _stream,
                        _group,
                        _consumer
                    );
                }

                var entries =
                    await database.StreamReadGroupAsync(
                        _stream,
                        _group,
                        _consumer,
                        ">",
                        count: 20
                    );

                if (entries.Length == 0)
                {
                    await Task.Delay(
                        _pollInterval,
                        stoppingToken
                    );

                    continue;
                }

                foreach (var entry in entries)
                {
                    await ProcessEntryAsync(
                        database,
                        entry,
                        stoppingToken
                    );
                }
            }
            catch (OperationCanceledException)
                when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
                when (
                    ex is RedisException
                    or RedisTimeoutException
                )
            {
                initialized = false;

                _logger.LogWarning(
                    "Redis unavailable. Worker will retry. Error={Error}",
                    ex.Message
                );

                try
                {
                    await Task.Delay(
                        TimeSpan.FromSeconds(3),
                        stoppingToken
                    );
                }
                catch (OperationCanceledException)
                    when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
            }
        }

        _logger.LogInformation(
            "Redis event worker stopped"
        );
    }

    private async Task EnsureConsumerGroupAsync(
        IDatabase database)
    {
        try
        {
            await database.StreamCreateConsumerGroupAsync(
                _stream,
                _group,
                "$",
                createStream: true
            );

            _logger.LogInformation(
                "Redis consumer group created. Group={Group}",
                _group
            );
        }
        catch (RedisServerException ex)
            when (ex.Message.Contains(
                "BUSYGROUP",
                StringComparison.OrdinalIgnoreCase))
        {
            _logger.LogDebug(
                "Redis consumer group already exists"
            );
        }
    }

    private async Task ProcessPendingMessagesAsync(
        IDatabase database,
        CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            var entries =
                await database.StreamReadGroupAsync(
                    _stream,
                    _group,
                    _consumer,
                    "0-0",
                    count: 20
                );

            if (entries.Length == 0)
            {
                return;
            }

            foreach (var entry in entries)
            {
                await ProcessEntryAsync(
                    database,
                    entry,
                    stoppingToken
                );
            }
        }
    }

    private async Task ProcessEntryAsync(
        IDatabase database,
        StreamEntry entry,
        CancellationToken stoppingToken)
    {
        stoppingToken.ThrowIfCancellationRequested();

        try
        {
            var fields = entry.Values.ToDictionary(
                pair => pair.Name.ToString(),
                pair => pair.Value.ToString()
            );

            fields.TryGetValue(
                "type",
                out var eventType
            );

            _dashboard.Add(
                new DashboardEvent(
                    entry.Id.ToString(),
                    eventType ?? "unknown",
                    DateTimeOffset.UtcNow,
                    fields
                )
            );

            switch (eventType)
            {
                case "whatsapp.message":
                {
                    var message = new MessageRecord
                    {
                        RedisEventId = entry.Id.ToString(),

                        WhatsAppMessageId =
                            GetField(fields, "message_id"),

                        ChatJid =
                            GetField(fields, "chat_jid"),

                        SenderJid =
                            GetField(fields, "sender_jid"),

                        Text =
                            GetField(fields, "text"),

                        FromMe =
                            GetField(fields, "from_me") == "1",

                        MessageType =
                            GetField(fields, "message_type"),

                        CreatedAt =
                            ParseCreatedAt(GetField(fields, "created_at")),

                        PersistedAt =
                            DateTimeOffset.UtcNow
                    };

                    var inserted = _messageStore.Save(message);

                    if (inserted)
                    {
                        _logger.LogInformation(
                            "WhatsApp message persisted. RedisEventId={RedisEventId} MessageId={MessageId}",
                            message.RedisEventId,
                            message.WhatsAppMessageId
                        );
                    }
                    else
                    {
                        _logger.LogInformation(
                            "WhatsApp message already persisted. RedisEventId={RedisEventId}",
                            message.RedisEventId
                        );
                    }

                    break;
                }

                case "contact.created":
                    _logger.LogInformation(
                        "Contact created on Android. Name={Name} Number={Number}",
                        Get(fields, "name"),
                        Get(fields, "number")
                    );

                    break;

                case "contact.failed":
                    _logger.LogError(
                        "Contact creation failed. Name={Name} Number={Number} Error={Error}",
                        Get(fields, "name"),
                        Get(fields, "number"),
                        Get(fields, "error")
                    );

                    break;

                default:
                    _logger.LogDebug(
                        "Redis event received. Id={Id} Type={Type}",
                        entry.Id,
                        eventType
                    );

                    break;
            }

            await database.StreamAcknowledgeAsync(
                _stream,
                _group,
                entry.Id
            );

            _logger.LogDebug(
                "Redis event acknowledged. EventId={EventId}",
                entry.Id
            );
        }
        catch (Exception ex)
        {
            _logger.LogError(
                ex,
                "Failed to process Redis event. EventId={EventId}. Event will not be acknowledged.",
                entry.Id
            );
        }
    }

    private static string GetField(
        Dictionary<string, string> fields,
        string name)
    {
        return fields.TryGetValue(name, out var value)
            ? value
            : string.Empty;
    }

    private static DateTimeOffset ParseCreatedAt(string value)
    {
        if (DateTimeOffset.TryParse(value, out var date))
        {
            return date;
        }

        if (long.TryParse(value, out var timestamp))
        {
            try
            {
                if (timestamp > 10_000_000_000)
                {
                    return DateTimeOffset.FromUnixTimeMilliseconds(timestamp);
                }

                return DateTimeOffset.FromUnixTimeSeconds(timestamp);
            }
            catch (ArgumentOutOfRangeException)
            {
                // usa fallback abaixo
            }
        }

        return DateTimeOffset.UtcNow;
    }

    private void LogWhatsAppMessage(
        RedisValue eventId,
        IReadOnlyDictionary<string, string> fields)
    {
        _logger.LogInformation(
            """
            WhatsApp message received
              EventId: {EventId}
              MessageId: {MessageId}
              Chat: {ChatJid}
              Sender: {SenderJid}
              FromMe: {FromMe}
              Type: {MessageType}
              Text: {Text}
            """,
            eventId,
            Get(fields, "message_id"),
            Get(fields, "chat_jid"),
            Get(fields, "sender_jid"),
            Get(fields, "from_me"),
            Get(fields, "message_type"),
            Get(fields, "text")
        );
    }

    private static string Get(
        IReadOnlyDictionary<string, string> fields,
        string key)
    {
        return fields.TryGetValue(
            key,
            out var value
        )
            ? value
            : "";
    }
}
