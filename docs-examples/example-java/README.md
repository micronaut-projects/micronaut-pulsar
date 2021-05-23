## Micronaut Pulsar Java Consumer Example with OAuth2 and transport encryption

This example shows how to implement simple Pulsar consumers with Micronaut by using most of the features required in a usual 
use case Pulsar message consuming. It covers OAuth2 in configuration, and consuming messages of different data types like String and user built classes.
It also has TLS for transport encryption support.

How to start:
1. Prepare dependencies:
    - A running instance of Apache Pulsar 2.7.0+ preconfigured for using OAuth2 as authentication and TLS certificates
    - A running instance of SSO like KeyCloak that supports OAuth2 which is used by Pulsar
2. Create Pulsar tenant called _private_, then create a namespace called _reports_
   - if Pulsar is left to default settings topic will be created automatically; otherwise create a topic _messages_
   - set tenant and namespace to be accessible by public or explicitly allow Pulsar user/role given to this API to subscribe to them
3. If anything is modified related to SSO user, update it in credentials.json
4. Copy or link path to certificate used with Pulsar server and change application.yaml accordingly (adjust port if necessary)
5. Run the project.

If everything is configured properly, WebSocket topic /ws/messages/public/default/messages
should print out messages from Pulsar while /ws/private/reports/messages should print out all those messages.
There's an HTTP (stream) endpoint which on access will start printing all new incoming messages into web browser.