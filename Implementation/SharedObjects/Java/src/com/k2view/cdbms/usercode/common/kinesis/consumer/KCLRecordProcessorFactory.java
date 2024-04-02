package com.k2view.cdbms.usercode.common.kinesis.consumer;


import com.k2view.fabric.common.Log;
import software.amazon.kinesis.common.StreamIdentifier;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

class KCLRecordProcessorFactory implements ShardRecordProcessorFactory {
    private final String applicationName;
    private final KCLApplication kclApp;
    KCLRecordProcessorFactory(String applicationName, KCLApplication kclApp) {
        this.applicationName = applicationName;
        this.kclApp = kclApp;
    }
    @Override
    public ShardRecordProcessor shardRecordProcessor() { return null; }

    @Override
    public ShardRecordProcessor shardRecordProcessor(StreamIdentifier streamIdentifier) {
        return new KCLRecordProcessor(streamIdentifier);
    }

    class KCLRecordProcessor implements ShardRecordProcessor {
        private final Log log = Log.a(KCLRecordProcessor.class);
        private String logSuffix;
        private String shardId;
        private final StreamIdentifier streamIdentifier;
        private final KinesisConsumer consumer;
        public KCLRecordProcessor(StreamIdentifier streamIdentifier) {
            this.streamIdentifier=streamIdentifier;
            this.consumer=kclApp.getConsumer();
        }

        /**
         * Invoked by the KCL before data records are delivered to the ShardRecordProcessor instance (via
         * processRecords).
         * @param initializationInput Provides information related to initialization.
         */
        public void initialize(InitializationInput initializationInput) {
            this.shardId = initializationInput.shardId();
            this.logSuffix = String.format(" (app=%s, stream=%s, shardId=%s)", applicationName, streamIdentifier.streamName(), shardId);
            log.debug("Initializing KCLRecordProcessor @ Sequence: {}{}", initializationInput.extendedSequenceNumber(), logSuffix);
        }

        /**
         * Handles record processing logic. The Amazon Kinesis Client Library will invoke this method to deliver
         * data records to the application.
         *
         * @param processRecordsInput Provides the records to be processed as well as information and capabilities
         *                            related to them (e.g. checkpointing).
         */
        public void processRecords(ProcessRecordsInput processRecordsInput) {
            log.debug("{} records awaiting processing..{}", processRecordsInput.records().size(), logSuffix);
            consumer.processRecords(shardId, processRecordsInput);
        }

        /** Called when the lease tied to this record processor has been lost. Once the lease has been lost,
         * the record processor can no longer checkpoint.
         *
         * @param leaseLostInput Provides access to functions and data related to the loss of the lease.
         */
        public void leaseLost(LeaseLostInput leaseLostInput) {
            log.debug("Lost lease, so terminating{}", logSuffix);
        }

        /**
         * Called when all data on this shard has been processed. Checkpointing must occur in the method for record
         * processing to be considered complete; an exception will be thrown otherwise.
         *
         * @param shardEndedInput Provides access to a checkpointer method for completing processing of the shard.
         */
        public void shardEnded(ShardEndedInput shardEndedInput) {
            try {
                log.debug("Reached shard end, so checkpointing.{}", logSuffix);
                // Since we must checkpoint, how does this affect transactions?
                shardEndedInput.checkpointer().checkpoint();
                consumer.removeCheckpointer(shardId);
            } catch (Exception e) {
                log.error("Exception while checkpointing at shard end. Giving up.{}", logSuffix);
                consumer.handleException(e);
            }
        }

        /**
         * Invoked when Scheduler has been requested to shut down.
         *
         * @param shutdownRequestedInput Provides access to a checkpointer, allowing a record processor to checkpoint
         *                               before the shutdown is completed.
         */
        public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
            log.debug("Scheduler is shutting down..{}", logSuffix);
        }
    }
}
