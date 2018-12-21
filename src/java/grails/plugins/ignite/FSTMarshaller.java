package grails.plugins.ignite;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.marshaller.MarshallerContext;
import org.jetbrains.annotations.Nullable;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * A marshaller that uses Fast Java Serialization
 * @see https://github.com/RuedigerMoeller/fast-serialization
 * Created by dstieglitz on 11/14/16.
 */
public class FSTMarshaller implements Marshaller {
    @Override
    public void setContext(MarshallerContext marshallerContext) {

    }

    @Override
    public void marshal(@Nullable Object o, OutputStream outputStream) throws IgniteCheckedException {

    }

    @Override
    public byte[] marshal(@Nullable Object o) throws IgniteCheckedException {
        return new byte[0];
    }

    @Override
    public <T> T unmarshal(InputStream inputStream, @Nullable ClassLoader classLoader) throws IgniteCheckedException {
        return null;
    }

    @Override
    public <T> T unmarshal(byte[] bytes, @Nullable ClassLoader classLoader) throws IgniteCheckedException {
        return null;
    }
}
