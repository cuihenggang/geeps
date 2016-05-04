# Compile and run

```
make
./helloworld
```

# Expected output

```
Finished "training", hello world!
[some error messages indicating that CUDA functions are called while the driver is shut down]

```

# Known issues

After the program finishes, there will be some error messages indicating that
CUDA functions are called while the driver is shut down.
This is because we are yet to implement the shutting-down procedures
of the background working threads.
