# NGX Storage Management API TLS

The NGX Storage NFS CSI driver communicates with the NGX Storage management API over HTTPS.

The current supported appliance compatibility model does not validate the
management API certificate. This allows deployments using locally managed or
self-signed NGX Storage certificates without distributing CA certificates
to CSI controller and node pods.

No CA Secret or TLS CLI configuration is required by the driver.

This behavior concerns only the NGX Storage management API connection. Kubernetes CSI
communication continues to use Unix domain sockets inside the CSI pods.
