using System.Text.RegularExpressions;
using StackExchange.Redis;
using WhatsAppBridge.Api.Models;
using WhatsAppBridge.Api.Redis;
using WhatsAppBridge.Api.Workers;
using WhatsAppBridge.Api.Data;
using WhatsAppBridge.Api.Dashboard;

var builder =
    WebApplication.CreateBuilder(args);

var redisConnectionString =
    builder.Configuration
        .GetConnectionString("Redis")
    ?? "127.0.0.1:6379,abortConnect=false";

builder.Services.AddSingleton<IConnectionMultiplexer>(
    _ =>
    {
        var options =
            ConfigurationOptions.Parse(
                redisConnectionString
            );

        return ConnectionMultiplexer.Connect(
            options
        );
    }
);

builder.Services.AddSingleton<
    RedisCommandPublisher
>();

builder.Services.AddSingleton<
    DatabaseInitializer
>();

builder.Services.AddSingleton<
    DashboardEventStore
>();

builder.Services.AddSingleton<
    MessageStore
 >();

builder.Services.AddHostedService<
    RedisEventWorker
>();

var app = builder.Build();

var databaseInitializer = app.Services.GetRequiredService<DatabaseInitializer>();

databaseInitializer.Initialize();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet(
    "/health",
    async (
        IConnectionMultiplexer redis
    ) =>
    {
        var latency =
            await redis
                .GetDatabase()
                .PingAsync();

        return Results.Ok(
            new
            {
                status = "ok",
                redis = "connected",
                latency_ms =
                    latency.TotalMilliseconds
            }
        );
    }
);

app.MapPost(
    "/contacts",
    async (
        CreateContactRequest request,
        RedisCommandPublisher publisher
    ) =>
    {
        var name =
            request.Name?.Trim();

        var number =
            request.Number?.Trim();

        if (string.IsNullOrWhiteSpace(name))
        {
            return Results.BadRequest(
                new
                {
                    error =
                        "name is required"
                }
            );
        }

        if (string.IsNullOrWhiteSpace(number))
        {
            return Results.BadRequest(
                new
                {
                    error =
                        "number is required"
                }
            );
        }

        if (!Regex.IsMatch(
            number,
            @"^\+?[0-9]{8,20}$"))
        {
            return Results.BadRequest(
                new
                {
                    error =
                        "invalid phone number"
                }
            );
        }

        var queueLength =
            await publisher
                .PublishCreateContactAsync(
                    name,
                    number
                );

        return Results.Accepted(
            value: new
            {
                status = "queued",
                name,
                number,
                queue_length = queueLength
            }
        );
    }
);

app.MapGet(
    "/api/dashboard",
    async (
        IConnectionMultiplexer redis,
        IConfiguration configuration,
        DashboardEventStore events
    ) =>
    {
        try
        {
        var database =
            redis.GetDatabase();

        var latency =
            await database.PingAsync();

        var commandsQueue =
            configuration[
                "Redis:CommandsQueue"
            ]
            ?? "android:commands";

        var queueLength =
            await database.ListLengthAsync(
                commandsQueue
            );

        return Results.Ok(
            new
            {
                status = "online",

                redis = new
                {
                    status = "connected",
                    latency_ms =
                        Math.Round(
                            latency.TotalMilliseconds,
                            2
                        )
                },

                commands = new
                {
                    queue = commandsQueue,
                    pending = queueLength
                },

                events = events.GetRecent(50)
            }
        );
        }
        catch (
            RedisConnectionException
        )
        {
            return Results.StatusCode(
                StatusCodes
                    .Status503ServiceUnavailable
            );
        }
    }
);

app.MapGet(
    "/api/messages",
    (
        MessageStore messageStore,
        int? limit
    ) =>
    {
        var requestedLimit =
            Math.Clamp(limit ?? 100, 1, 500);

        var messages =
            messageStore.GetMessages(requestedLimit);

        return Results.Ok(messages);
    }
);

app.Run();
