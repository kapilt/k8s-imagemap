
# K8S ImageMap


WORK IN PROGRESS 

A cli for rewriting manifests and republishing images.


So a common problem in consuming manifests is that they may reference images with repositories that
may not be viable for the consumer (policy, network restriction, etc). This cli provides a solution
wherein the underlying images in the manifests can be republished to a different repository, and the
manifests rewritten to reference those locations.


Republishing images
```
python imagemap.py republish -f ~/myk8s-manifests/statefulsets/ -r 644160558196.dkr.ecr.us-east-1.amazonaws.com
```

Deploying via kubectl

```
python imagemap.py 
```
