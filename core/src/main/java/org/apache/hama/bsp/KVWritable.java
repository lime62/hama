
package org.apache.hama.bsp;

import com.google.common.base.Objects;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hama.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class KVWritable<K extends WritableComparable<? super K>, V extends Writable>
    implements WritableComparable<KVWritable<K, V>> {

  private K key;
  private V value;

  public KVWritable() {
  }

  public KVWritable(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public K getKey() {
    return key;
  }

  public void setKey(K key) {
    this.key = key;
  }

  public V getValue() {
    return value;
  }

  public void setValue(V val) {
    this.value = val;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeUTF(getKey().getClass().getName());
    getKey().write(out);
    out.writeUTF(getValue().getClass().getName());
    getValue().write(out);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void readFields(DataInput in) throws IOException {
    String keyClassName = in.readUTF();
    try {
      setKey((K) ReflectionUtils.newInstance(keyClassName));
      getKey().readFields(in);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }

    String valueClassName = in.readUTF();
    try {
      setValue((V) ReflectionUtils.newInstance(valueClassName));
      getValue().readFields(in);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(key);
  }

  @Override
  public int compareTo(KVWritable<K, V> that) {
    return getKey().compareTo(that.getKey());
  }
}