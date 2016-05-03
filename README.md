# GeePS

GeePS is a parameter server library that scales single-machine GPU machine learning applications (such as Caffe) to a cluster of machines.


## Download and build GeePS and Caffe application

Run the following command to download GeePS and (our slightly modified) Caffe:

```
git clone --recurse-submodules https://github.com/cuihenggang/geeps.git
```

If you use the Ubuntu 14.04 system, you can run the following commands (from geeps root directory) to install the dependencies:

```
./scripts/install-geeps-deps-ubuntu14.sh
./scripts/install-caffe-deps-ubuntu14.sh
```

After installing the dependencies, you can build GeePS by simply running this command from geeps root directory:

```
scons -j8
```

You can then build (our slightly modified) Caffe by first entering the `apps/caffe` directory and then running `make -j8`:

```
cd apps/caffe
make -j8
```


## Caffe's CIFAR-10 example on two machines

You can run Caffe distributedly across a cluster of machines with GeePS. In this section, we will show you the steps to run Caffe's CIFAR-10 example on two machines.

All commands in this part is executed from the `apps/caffe` directory:

```
cd apps/caffe
```

You will need to prepare a machine file in `examples/cifar10/2parts', with each line being the host name of one machine. We will use `pdsh' to launch commands on those machines with `ssh' protocol, so please make sure you can `ssh' to those machines without password.

When you have your machine file in ready, you can run the following command to download and prepare the CIFAR-10 dataset:

```
./data/cifar10/get_cifar10.sh
./examples/cifar10/2parts/create_cifar10_pdsh.sh
```

You can then training an Inception network on CIFAR-10 data with this command:

```
./examples/cifar10/2parts/train_inception.sh
```

Happy training!


## Reference Paper

Henggang Cui, Hao Zhang, Gregory R. Ganger, Phillip B. Gibbons, and Eric P. Xing.
[GeePS: Scalable Deep Learning on Distributed GPUs with a GPU-Specialized Parameter Server](https://users.ece.cmu.edu/~hengganc/archive/paper/[eurosys16]geeps.pdf).
In ACM European Conference on Computer Systems, 2016 (EuroSys'16)
