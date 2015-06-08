package org.grails.ignite;

import com.cedarsoftware.util.io.GroovyJsonReader;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerContext;
import org.apache.ignite.marshaller.optimized.OptimizedMarshaller;
import org.apache.ignite.marshaller.optimized.OptimizedMarshallerIdMapper;
import org.jetbrains.annotations.Nullable;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * <b>Work-in-progress</b>
 * <p>
 *     This class is a custom marshaller that decorates the Ignite optimized marshaller to support the transmission
 *     of anonymous classes or closures through the Grid. At the time of this writing if classes are not explicitly
 *     defined in class files they cannot be peer-loaded, so if there is an anonymous closure defined somewhere it
 *     cannot be executed on any compute grids.
 * </p>
 * @author Dan Stieglitz
 */
public class GroovyOptimizedMarshallerDecorator implements Marshaller {

    private OptimizedMarshaller underlyingMarshaller;

    public static boolean available() {
        return OptimizedMarshaller.available();
    }

    public void setUnderlyingMarshaller(OptimizedMarshaller optimizedMarshaller) {
        this.underlyingMarshaller = optimizedMarshaller;
    }

    public void setRequireSerializable(boolean requireSer) {
        underlyingMarshaller.setRequireSerializable(requireSer);
    }

    public void setIdMapper(OptimizedMarshallerIdMapper mapper) {
        underlyingMarshaller.setIdMapper(mapper);
    }

    public void setPoolSize(int poolSize) {
        underlyingMarshaller.setPoolSize(poolSize);
    }

    public void onUndeploy(ClassLoader ldr) {
        underlyingMarshaller.onUndeploy(ldr);
    }

    @Override
    public void marshal(@Nullable Object obj, OutputStream out) throws IgniteCheckedException {
        System.out.println("MARSHALL -->" + obj.getClass().getName() + " to " + out);
        System.out.println("MARSHAL CONTENTS: '"+obj+"'");
        underlyingMarshaller.marshal(obj, out);
    }

    @Override
    public byte[] marshal(@Nullable Object obj) throws IgniteCheckedException {
        if (obj!=null && obj.getClass()==null) {
            System.out.println("MARSHALL -->" + obj.getClass().getName() + " to byte array");
            System.out.println("MARSHAL CONTENTS: '"+obj+"'");
        }
        return underlyingMarshaller.marshal(obj);
    }

    @Override
    public <T> T unmarshal(InputStream in, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        System.out.println("unmarshall " + in + "," + clsLdr);
//        try {
            return underlyingMarshaller.unmarshal(in, clsLdr);
//        } catch (Throwable e) {
//            GroovyJsonReader jr = new GroovyJsonReader(in);
//            Object obj = jr.readObject();
//            System.out.println(obj);
//            System.exit(0);
////            return obj.getClass().cast(obj);
//            return null;
//        }
    }

    @Override
    public <T> T unmarshal(byte[] arr, @Nullable ClassLoader clsLdr) throws IgniteCheckedException {
        System.out.println("unmarshall " + new String(arr) + "," + clsLdr);
//        try {
            return underlyingMarshaller.unmarshal(arr, clsLdr);
//        } catch (Throwable e) {
//            ByteArrayInputStream bais = new ByteArrayInputStream(arr);
//            GroovyJsonReader jr = new GroovyJsonReader(bais);
//            Object obj = jr.readObject();
//            System.out.println(obj);
//            System.exit(0);
////            return obj.getClass().cast(obj);
//            return null;
//        }
    }

    @Override
    public void setContext(MarshallerContext ctx) {
        underlyingMarshaller.setContext(ctx);
    }
}
