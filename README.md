# netq

This is aiming to become a simple alternative for streaming platforms like NATS,
just with a minimal set of features that follow the principles of least surprise
while still being performing well.

Features:

* Small codebase
* Batch processing
* Priority Queues
* Multiple consumers
* Integrates nicely with existing (quasi-)standards (Websockets, Prometheus)
* Manual acknowledge
* Integrated command line tool to help with most things


TODO: Mention data safety -> needs client side queue to be sure.


## Prometheus exporter

``netq`` implements a native prometheus exporter exposed at port TODO
The following metrics are being send out:
