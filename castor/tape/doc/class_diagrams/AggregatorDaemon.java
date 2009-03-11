/**
 */
class BaseDaemon {
}


/**
 */
class TCPListenerThreadPool {
}


/**
 * @opt operations
 */
class DriveAllocationProtocolEngine {
public void run();
}


/**
 * @opt operations
 */
class BridgeProtocolEngine {
public void run();
}


/**
 * @opt operations
 */
class Builder {
public void run();
}


/**
 * @composed 1 - 1 TCPListenerThreadPool
 * @composed 1 - 1 DriveAllocationProtocolEngine
 * @composed 1 - 1 BridgeProtocolEngine
 * @composed 1 - 1 Builder
 */
class AggregatorDaemon extends BaseDaemon {
}
