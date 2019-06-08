package com.github.pwliwanow.foundationdb4s.schema
import java.nio.ByteBuffer

import com.apple.foundationdb.tuple.Tuple
import shapeless.{::, Generic, HList, HNil, Lazy}

import scala.collection.mutable.ListBuffer
import scala.util.Try

object DefaultCodecs extends DefaultCodecs

trait DefaultCodecs extends DefaultEncodersProtocol with DefaultDecodersProtocol

trait TupleEncoder[A] { self =>
  def encode(value: A): Tuple
  def encode(value: A, acc: Tuple, preceedingNulls: Int): (Tuple, Int)

  def contramap[B](f: B => A): TupleEncoder[B] = new TupleEncoder[B] {
    override def encode(value: B): Tuple = self.encode(f(value))
    override def encode(value: B, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
      self.encode(f(value), acc, preceedingNulls)

  }
}

trait DefaultEncodersProtocol extends BasicEncodersProtocol with DerivationEncoders

trait BasicEncodersProtocol {
  import EncodersHelpers._

  implicit object IntEncoder extends TupleEncoder[Int] {
    override def encode(value: Int): Tuple = new Tuple().add(encodeInt(value))
    override def encode(value: Int, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
      (addNulls(acc, preceedingNulls).add(encodeInt(value)), 0)
  }

  implicit object LongEncoder extends TupleEncoder[Long] {
    override def encode(value: Long): Tuple = new Tuple().add(value)
    override def encode(value: Long, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
      (addNulls(acc, preceedingNulls).add(value), 0)
  }

  implicit object StringEncoder extends TupleEncoder[String] {
    override def encode(value: String): Tuple = new Tuple().add(value)
    override def encode(value: String, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
      (addNulls(acc, preceedingNulls).add(value), 0)
  }

  implicit object BooleanEncoder extends TupleEncoder[Boolean] {
    override def encode(value: Boolean): Tuple = new Tuple().add(value)
    override def encode(value: Boolean, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
      (addNulls(acc, preceedingNulls).add(value), 0)
  }

  implicit def optionEncoder[A](implicit ev: TupleEncoder[A]): TupleEncoder[Option[A]] =
    new TupleEncoder[Option[A]] {
      override def encode(value: Option[A]): Tuple =
        value.fold(new Tuple().addObject(null))(ev.encode)
      override def encode(value: Option[A], acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
        value.fold((acc, preceedingNulls + 1))(ev.encode(_, acc, preceedingNulls))
    }

  // todo maybe deserialize it through option?
  // fixme it won't work for List[None]
  implicit def listEncoder[A](implicit ev: TupleEncoder[A]): TupleEncoder[List[A]] =
    new TupleEncoder[List[A]] {
      override def encode(value: List[A]): Tuple =
        encode(value, new Tuple(), 0)._1

      override def encode(value: List[A], acc: Tuple, preceedingNulls: Int): (Tuple, Int) = {
        if (value.isEmpty) (acc, preceedingNulls + 1)
        else {
          val listTuple = value.foldLeft(new Tuple())((acc, a) => ev.encode(a, acc, 0)._1)
          (addNulls(acc, preceedingNulls).add(listTuple), 0)
        }
      }
    }

  private def encodeInt(value: Int): Array[Byte] = {
    val output = new Array[Byte](4)
    ByteBuffer.wrap(output).putInt(value)
    output
  }
}

trait DerivationEncoders {
  import EncodersHelpers._

  implicit object HnilEncoder extends TupleEncoder[HNil] {
    override def encode(value: HNil): Tuple = new Tuple()
    override def encode(value: HNil, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
      (acc, preceedingNulls)
  }

  implicit def hlistEncoder[H, T <: HList](
      implicit hEncoder: TupleEncoder[H],
      tEncoder: TupleEncoder[T]): TupleEncoder[H :: T] = new TupleEncoder[H :: T] {
    override def encode(value: H :: T): Tuple = {
      val (res, _) = encode(value, new Tuple(), 0)
      res
    }
    override def encode(value: H :: T, acc: Tuple, preceedingNulls: Int): (Tuple, Int) = {
      val h :: t = value
      val (hEncoded, numberOfNulls) = hEncoder.encode(h, acc, preceedingNulls)
      tEncoder.encode(t, hEncoded, numberOfNulls)
    }
  }

  implicit def genericEncoder[A, R](
      implicit gen: Generic.Aux[A, R],
      enc: Lazy[TupleEncoder[R]]): TupleEncoder[A] = new TupleEncoder[A] {
    override def encode(value: A): Tuple = {
      val (res, _) = enc.value.encode(gen.to(value), new Tuple, 0)
      res
    }
    override def encode(value: A, acc: Tuple, preceedingNulls: Int): (Tuple, Int) =
      (addNulls(acc, preceedingNulls).add(encode(value)), 0)
  }

}

object EncodersHelpers {
  def addNulls(tuple: Tuple, preceedingNulls: Int): Tuple = {
    var acc = tuple
    var i = 0
    while (i < preceedingNulls) {
      acc = acc.addObject(null)
      i += 1
    }
    acc
  }
}

trait TupleDecoder[A] { self =>
  def decode(tuple: Tuple): A
  def decode(tuple: Tuple, index: Int): A

  def map[B](f: A => B): TupleDecoder[B] = new TupleDecoder[B] {
    override def decode(tuple: Tuple): B = f(self.decode(tuple))
    override def decode(tuple: Tuple, index: Int): B = f(self.decode(tuple, index))
  }
}

trait DefaultDecodersProtocol extends BasicDecodersProtocol with DerivationDecoders

trait BasicDecodersProtocol {

  implicit object IntDecoder extends TupleDecoder[Int] {
    override def decode(tuple: Tuple): Int = decode(tuple, 0)
    override def decode(tuple: Tuple, index: Int): Int = decodeInt(tuple.getBytes(index))
  }

  implicit object LongDecoder extends TupleDecoder[Long] {
    override def decode(tuple: Tuple): Long = decode(tuple, 0)
    override def decode(tuple: Tuple, index: Int): Long = tuple.getLong(index)
  }

  implicit object StringDecoder extends TupleDecoder[String] {
    override def decode(tuple: Tuple): String = decode(tuple, 0)
    override def decode(tuple: Tuple, index: Int): String = tuple.getString(index)
  }

  implicit object BooleanDecoder extends TupleDecoder[Boolean] {
    override def decode(tuple: Tuple): Boolean = decode(tuple, 0)
    override def decode(tuple: Tuple, index: Int): Boolean = tuple.getBoolean(index)
  }

  implicit def optionDecoder[A](implicit ev: TupleDecoder[A]): TupleDecoder[Option[A]] =
    new TupleDecoder[Option[A]] {
      override def decode(tuple: Tuple): Option[A] = decode(tuple, 0)

      override def decode(tuple: Tuple, index: Int): Option[A] = {
        Try(Option(ev.decode(tuple, index))).recover {
          case _: NullPointerException      => None
          case _: IndexOutOfBoundsException => None
        }.get
      }
    }

  implicit def listDecoder[A](implicit ev: TupleDecoder[A]): TupleDecoder[List[A]] =
    new TupleDecoder[List[A]] {
      override def decode(tuple: Tuple): List[A] = decode(tuple, 0)
      override def decode(tuple: Tuple, index: Int): List[A] = {
        val result = Try {
          val nested = tuple.getNestedTuple(index)
          val acc = ListBuffer.empty[A]
          for (i <- 0 until nested.size()) {
            acc += ev.decode(nested, i)
          }
          acc.toList
        }
        result.recover {
          case _: NullPointerException      => List.empty
          case _: IndexOutOfBoundsException => List.empty
        }.get
      }
    }

  private def decodeInt(value: Array[Byte]): Int = {
    if (value.length != 4) throw new IllegalArgumentException("Array must be of size 4")
    ByteBuffer.wrap(value).getInt
  }

}

trait DerivationDecoders {
  implicit val hnilDecoder: TupleDecoder[HNil] = new TupleDecoder[HNil] {
    override def decode(tuple: Tuple): HNil = HNil
    override def decode(tuple: Tuple, index: Int): HNil = HNil
  }

  implicit def hlistDecoder[H, T <: HList](
      implicit hDecoder: TupleDecoder[H],
      tDecoder: TupleDecoder[T]): TupleDecoder[H :: T] = new TupleDecoder[H :: T] {
    override def decode(tuple: Tuple): H :: T = decode(tuple, 0)
    override def decode(tuple: Tuple, index: Int): H :: T = {
      val h = hDecoder.decode(tuple, index)
      val t = tDecoder.decode(tuple, index + 1)
      h :: t
    }
  }

  implicit def genericDecoder[A, R](
      implicit gen: Generic.Aux[A, R],
      dec: Lazy[TupleDecoder[R]]): TupleDecoder[A] = new TupleDecoder[A] {
    // decode directly from tuple
    override def decode(tuple: Tuple): A = gen.from(dec.value.decode(tuple, 0))
    override def decode(tuple: Tuple, index: Int): A = {
      // whole "class" is wrapped in a tuple
      val nested = tuple.getNestedTuple(index)
      decode(nested)
    }
  }

}
