/**
 */
class BaseDaemon {
}


/**
 */
class IThread {
}


/**
 * @assoc 1 - 1 DriveAllocationProtocolEngine
 * @assoc 1 - 1 Packer
 * @assoc 1 - 1 Unpacker
 * @assoc 1 - 1 BridgeProtocolEngine
 */
class VdqmRequestHandler extends IThread {
}


/**
 * @composed 1 - 1 VdqmRequestHandler
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
class Unpacker {
public void run();
}


/**
 * @composed 1 - 1 TCPListenerThreadPool
 */
class AggregatorDaemon extends BaseDaemon {
}
