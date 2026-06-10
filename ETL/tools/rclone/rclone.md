    rclone sync "veto:veto-stream-logs/veto-stream-logs/06/09" "Y:\Veto Logs Backup\Veto Stream Logs\06\09" --size-only --transfers 16 --checkers 32 --multi-thread-streams 4 --buffer-size 16M -P
    rclone sync "veto:veto-stream-logs/veto-fast-logs/06/09" "Y:\Veto Logs Backup\Veto fast Logs\06\09" --size-only --transfers 16 --checkers 32 --multi-thread-streams 4 --buffer-size 16M -P
