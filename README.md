# Distributed Systems Project

The repository for the DS project.

Authors:

- [Leonardo De Faveri](https://github.com/LeonardoDeFaveri)
- [Lorenzo Fumi](https://github.com/DeeJack)

## Requirements

- Gradle
- Java

## Getting started

- Clone the repository: `git clone https://github.com/LeonardoDeFaveri/DistributedSystemsProject`
- Enter the folder with the terminal: `cd DistributedSystemsProject`
- Run the application with gradle (if gradle is in the environment variables): `gradle run`

## Architectural choices

- Both replicas and clients hold a list of all the replicas in the system:
  - Replicas need to know which are the other replicas
  - Clients pick the replica to contact from that list. Each client has a
    favorite replica and keeps contacting it until it crashes. When that happens,
    it picks another one
- Initially the coordinator is the first replica created
- Replicas keep track of the last time they heard anythig from the coordinator.
HearbetMsg and any other kind of message received from the coordinator makes them
reset this time of last contact

## Running the system
- Start the system with `gradle run`
  Replicas and clients are created
- Press `ENTER` to send a start signal to all hosts:
  - Clients begin sending requests to replicas
  - Replicas begin expecting heartbeat messages (or ay kind of message) from the
  coordinator
  - Coordinator begin sending heartbeat messages to replicas
- Press `ENTER` again to send a stop signal to clients forcing them to stop
producing more requests
- Press `ENTER` a third time to terminate the system

## Crash detection protocol
Crash detection works by means of periodic checks of arrival of messages.

### Detection of crashed replicas by clients
- Read requests: `ReadMsg`

  Each clients identifies univocally a read request by means of an incrementing
  index that's placed withing the request itself. Upon sending the request to a
  replica, the client registers the sent request into `readMsgs` (maps read
  indexes to replicas) and starts a timeout (`READOK_TIMEOUT`). When the
  timeout expires a `ReadOkReceivedMsg` is sent by the client to itself.

  A client expects to receive a `ReadOkMsg` from the contacted replica and that
  message should contain the read value and the Id of the served read request. On
  arrival, the client removes from `readMsgs` the request with the ID found in
  the `ReadOkMsg`. A `ReadOkReceivedMsg` also contains the ID of the associated
  request and the handler checks if `readMsgs` holds a value. If that's the case,
  then it means that no `ReadOkMsg` has been received so the replica found in the
  map associated to that ID is presumed to be crashed and removed from the list
  of active replicas. Otherwise, if the map has no value it means that the
  request has been served and the replica was fine up to that point.

- Update requests: `UpdateRequestMsg`

  An update request is identified by the `ActorRef` of the client and another
  incrementing ID. Such pair is handled by the `UpdateRequestId` class. As for
  read requests, when a client sends un update to a replica, it registers the
  request and the destination replica in a map (`writeMsgs`) using the ID as key
  and starts a timeout (`UpdateRequestOkReceivedMsg`).

  The replica, once the request has been served, responds with an
  `UpdateRequestOkMsg` holding the index of the served request. On arrival, the
  index found is used to remove the associated value from `writeMsgs` and on
  arrival of the corresponding `UpdateRequestOkMsg` the same check done for
  read requests is performed. So, a no-value means that the request has been
  served and otherwise that the contacted replica has taken too long to answer
  and is identified as crashed.

  On replica side, when an update request is received the ID (pair `<client, index>`)
  is saved in the set `updateRequests`. This ID is also put into all associated
  `WriteMsg`s and `WriteOkMsg`s. When the update protocol terminates and a
  replica receives a `WriteOkMSg`, it takes the update request ID carried by the
  message and checks if it is present into `updateRequests`. If that's the case,
  it means that this replica is the one that has received the request from the
  client and thus it has to send back an ACK, namely an `UpdateRequestOkMsg` to
  that client. This message holds the local update request index of that client.

### Detection of crashed coordinator by replicas

- On update requests: `UpdateRequestMsg`

  When a replica that's not the coordinator receives an update request, forwards
  it to the coordinator and expects to recive back a `WriteMsg`. So, upon forwarding
  the request the replica start a timeout (`WRITEMSG_TIMEOUT`) and registers the
  ID of the request into the `pendingUpdateRequests` set.

  When the coordinator sends a `WriteMsg` it embeds in it the `updateRequestId` of
  the update request that's being served. Upon reception of this message by the
  coordinator, a replica removes the `updateRequestId` from `pendingUpdateRequests`
  and adds the `WriteId` of the message into `writeRequests` to register the serving
  of that write request.

  When `WRITEMSG_TIMEOUT` expires a `WriteMsgReceivedMsg` is sent by the replica to
  itself and if the expected `WriteMsg` has not been received, so if
  `pendingUpdateRequests` has `updateRequestId` in it, the coordinator is considered
  to be crashed. Alive, otherwise.

  On coordinator side, when an `UpdateRequestMsg` is received, the association
  `WriteId->UpdateRequestId`, `WriteId` being a class representing the pair
  `<epoch, write_index>`, is saved into `writesToUpates` maps and is later used
  to build the `WriteOkMsg`s.

- On write message: `WriteMsg`

  When a replica receives a `WriteMsg` it sends an ACK back to the coordinator
  and expects to receive a `WriteOkMsg` in response. So, again a timer
  (`WRITEOK_TIMEOUT`) is set and a `WriteOkReceivedMSg` is sent by a replica to
  itself at expiration. On receipt of a `WriteOk`, the associated `WriteId` is
  removed from `writeRequests` and on receipt of `WriteOkRecivedMsg` the same
  kind of cheks done for the others `ReceivedMsg`s is done. So, if `writeRequests`
  still holds `WriteId`, the request has not been served yet, and the coordinator
  is set to crashed.

  On coordinator side, when enough ACKs are received for a request and the
  corresponing `WriteOkMsg` can be sent, the `WriteId` associated by current
  values for `epoch` and `currentWriteToAck` is used to retrive from
  `writesToUpdates` to `UpdateRequestId` of originating request and such ID is
  put into the `WriteOk`.

### Detection of crashed replicas by coordinator
- On ACK message: `WriteAckMsg`

  Thanks to the assumption on the minimum available number of a replicas, no
  crash check needs to be perfomed by coordinator side on recipt of ACK messages.