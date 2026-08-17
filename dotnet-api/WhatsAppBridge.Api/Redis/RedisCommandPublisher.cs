using System.Text.Json;
using StackExchange.Redis;

namespace WhatsAppBridge.Api.Redis;

public sealed class RedisCommandPublisher
{
    private readonly IDatabase _database;
    private readonly RedisKey _commandsQueue;

    public RedisCommandPublisher(
        IConnectionMultiplexer redis,
        IConfiguration configuration)
    {
        _database = redis.GetDatabase();

        _commandsQueue =
            configuration["Redis:CommandsQueue"]
            ?? "android:commands";
    }

    public async Task<long> PublishCreateContactAsync(
        string name,
        string number)
    {
        var payload = JsonSerializer.Serialize(
            new
            {
                type = "contact.create",
                name,
                number
            }
        );

        return await _database.ListRightPushAsync(
            _commandsQueue,
            (RedisValue)payload
        );
    }
}

