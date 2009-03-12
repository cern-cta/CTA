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
class Packer {
public void run();
}


/**
 * @opt operations
 */
class Recaller {
public void run();
}


/**
 * @composed 1 - 1 TCPListenerThreadPool
 * @composed 1 - 1 DriveAllocationProtocolEngine
 * @composed 1 - 1 BridgeProtocolEngine
 * @composed 1 - 1 Packer
 * @composed 1 - 1 Recaller
 */
class AggregatorDaemon extends BaseDaemon {
}
