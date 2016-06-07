# sbraft


Install ruby

```
sudo apt-add-repository ppa:brightbox/ruby-ng

sudo apt-get update

sudo apt-get install ruby2.3 ruby2.3-dev
```

Install RabbitMQ gem

```
sudo gem install bunny --version ">= 2.2.2"
```

Install Bundler

```
sudo apt-get -y install bundler
```

To start a data center program on a machine, please copy the codes to the datacenter and run

```
./dc1.rb
```

This will start dc1 on this machine.

Similarly, you can start dc2 by `./dc2.rb`, dc3 by `./dc3.rb`, etc

To start a client connecting to dc1, run `c1.rb`:

```
./c1.rb
```

Or a client connecting to dc2 by

```
./c2.rb
```