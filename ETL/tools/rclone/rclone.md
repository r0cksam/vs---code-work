    rclone sync "veto:veto-stream-logs/veto-stream-logs/06/04" "Y:\Veto Logs Backup\Veto Stream Logs\06\04" --size-only --transfers 16 --checkers 32 --multi-thread-streams 4 --buffer-size 16M -P
    rclone sync "veto:veto-stream-logs/veto-fast-logs/06/02" "Y:\Veto Logs Backup\Veto fast Logs\06\02" --size-only --transfers 16 --checkers 32 --multi-thread-streams 4 --buffer-size 16M -P
