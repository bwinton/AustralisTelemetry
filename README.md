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