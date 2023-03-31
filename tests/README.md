Run drill by using ```drill --benchmark tests/testname.yml --stats```

Run goose by building, then running 
```cargo run --release -- -H http://ADDRESS --startup-time XXs --users XX --run-time XXs --no-reset-metrics```
