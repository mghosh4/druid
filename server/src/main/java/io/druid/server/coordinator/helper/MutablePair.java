package io.druid.query;

import com.google.common.base.Function;
import java.util.Comparator;

public class MutablePair<T1, T2> {
  public T1 lhs;
  public T2 rhs;

  public static <T1, T2> MutablePair<T1, T2> of(T1 lhs, T2 rhs) {
    return new MutablePair(lhs, rhs);
  }

  public MutablePair(T1 lhs, T2 rhs) {
    this.lhs = lhs;
    this.rhs = rhs;
  }

  public boolean equals(Object o) {
    if(this == o) {
      return true;
    } else if(o != null && this.getClass() == o.getClass()) {
      MutablePair pair = (MutablePair)o;
      if(this.lhs != null) {
        if(!this.lhs.equals(pair.lhs)) {
          return false;
        }
      } else if(pair.lhs != null) {
        return false;
      }

      if(this.rhs != null) {
        if(this.rhs.equals(pair.rhs)) {
          return true;
        }
      } else if(pair.rhs == null) {
        return true;
      }

      return false;
    } else {
      return false;
    }
  }

  public int hashCode() {
    int result = this.lhs != null?this.lhs.hashCode():0;
    result = 31 * result + (this.rhs != null?this.rhs.hashCode():0);
    return result;
  }

  public String toString() {
    return "Pair{lhs=" + this.lhs + ", rhs=" + this.rhs + '}';
  }

  public static <T1, T2> Function<MutablePair<T1, T2>, T1> lhsFn() {
    return new Function<MutablePair<T1, T2>, T1>() {
      public T1 apply(MutablePair<T1, T2> input) {
        return input.lhs;
      }
    };
  }

  public static <T1, T2> Function<MutablePair<T1, T2>, T2> rhsFn() {
    return new Function<MutablePair<T1, T2>, T2>() {
      public T2 apply(MutablePair<T1, T2> input) {
        return input.rhs;
      }
    };
  }

  public static <T1> Comparator<MutablePair<T1, ?>> lhsComparator(final Comparator<T1> comparator) {
    return new Comparator<MutablePair<T1, ?>>() {
      public int compare(MutablePair<T1, ?> o1, MutablePair<T1, ?> o2) {
        return comparator.compare(o1.lhs, o2.lhs);
      }
    };
  }
}