using System.Collections.Concurrent;

namespace WhatsAppBridge.Api.Dashboard;

public sealed class DashboardEventStore
{
    private const int MaxEvents = 200;

    private readonly ConcurrentQueue<DashboardEvent> _events = new();

    public void Add(DashboardEvent dashboardEvent)
    {
        _events.Enqueue(dashboardEvent);

        while (_events.Count > MaxEvents)
        {
            _events.TryDequeue(out _);
        }
    }

    public IReadOnlyList<DashboardEvent> GetRecent(
        int limit = 50)
    {
        return _events
            .Reverse()
            .Take(limit)
            .ToArray();
    }
}

