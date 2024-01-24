/////////////////////////////////////////////////////////////////////////
// Project Shared Functions
/////////////////////////////////////////////////////////////////////////

package com.k2view.cdbms.usercode.common.kinesis;
import com.k2view.fabric.common.io.IoProvider;

@SuppressWarnings({"unused", "DefaultAnnotationParam"})
public class SharedLogic {
    public static IoProvider kinesisIoProvider() {
        return new KinesisIoProvider();
    }
}
