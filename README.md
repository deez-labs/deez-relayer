# Running a Relayer

## Setup

See https://jito-foundation.gitbook.io/mev/jito-relayer/running-a-relayer until Section "RPC Server" for setup instructions.

--> Create a new folder (e.g. "deez-relayer") and add your jito-relayer authentication files: public.pem, private.pem, keypair.json

## Run

The easiest way to run the Deez Relayer is to run our public docker image.

Example run command with a deez-relayer at /home/ubuntu/deez-relayer authentication folder and a local RPC running:

````shell
sudo docker run -d -v /home/ubuntu/deez-relayer/:/home/ubuntu/deez-relayer \
    --name=deez-relayer \
    --restart=always \
    --network=host \
    --entrypoint=./jito-transaction-relayer deezlabs/deez-relayer:latest \
    --rpc-servers="http://127.0.0.1:8899" \
    --websocket-servers="ws://127.0.0.1:8900" \
    --keypair-path=/home/ubuntu/deez-relayer/keypair.json \
    --signing-key-pem-path=/home/ubuntu/deez-relayer/private.pem \
    --verifying-key-pem-path=/home/ubuntu/deez-relayer/public.pem \
    --block-engine-url=https://amsterdam.mainnet.block-engine.jito.wtf \
    --deez-engine-url=<redacted> \
    --packet-delay-ms=200 \
    --public-ip 178.0.0.0
```
````
