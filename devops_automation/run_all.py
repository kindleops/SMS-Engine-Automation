from . import health_poll, redis_sync, render_sync, kpi_daily

def run():
    out = {
        "health": health_poll.run(),
        "redis": redis_sync.run(),
        "render": render_sync.run(),
        "kpi": kpi_daily.run(),
    }
    return out

if __name__ == "__main__":
    print(run())