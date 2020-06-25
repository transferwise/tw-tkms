# tw-tkms

MariaDb:
auto increment: 2

useCompression=true results better performance on large (100k+ messages).
`ENGINE=InnoDB PAGE_COMPRESSED=1` on table makes it even faster.