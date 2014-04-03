AustralisTelemetry
==================

A repo to store the telemetry parse scripts I’m using…

Includes the [telemetry-server](https://github.com/mozilla/telemetry-server.git).

To use (instructions modified from [mreid’s blog post](http://mreid-moz.github.io/blog/2013/11/06/current-state-of-telemetry-analysis/)):

```Shell
tmux
sudo mkdir /mnt/telemetry
sudo chown ubuntu:ubuntu /mnt/telemetry
cd /mnt/telemetry
git clone https://github.com/bwinton/AustralisTelemetry.git .
mkdir -p work/cache
cd telemetry-server
../run.sh -v
```

If you’re running it not on a VM, you may also need to do the following before `cd telemetry-server`:

```Shell
easy_install pip
pip install boto
```

And create a `~/.boto` file with the contents:
```INI
[Credentials]
aws_access_key_id = <Your AWS Key>
aws_secret_access_key = <Your AWS Secret>
```