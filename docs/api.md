#### `GET /events?page=2&limit=100`
Get paginated events ordered by time

#### `POST /events`
Create even
```json
{
  "user_id": 1234,
  "event_type": "user.updated",
  "timestamp": "2025-05-28T12:34:56Z",
  "metadata": {
    "page": "/home"
  }
}
```

#### `GET /users/{user_id}/events`
Get last 1000 events of user

#### `GET /stats?from=2025-05-28T12:34:56Z&to=2025-05-28T12:34:56Z&e_type=user.updated`
Get stats by time