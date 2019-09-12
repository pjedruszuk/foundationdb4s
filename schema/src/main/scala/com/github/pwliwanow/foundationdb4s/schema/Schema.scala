package com.github.pwliwanow.foundationdb4s.schema
import com.apple.foundationdb.{KeySelector, Range}
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.foundationdb4s.core.{DBIO, ReadDBIO, TypedSubspace}
import shapeless.{=:!=, Generic, HList}

import scala.collection.immutable.Seq

object Schema {
  type Aux[Key <: HList, Value <: HList] = Schema { type KeySchema = Key; type ValueSchema = Value }
}

trait Schema {
  type KeySchema <: HList
  type ValueSchema <: HList

  abstract class SchemaBasedSubspace[Entity, Key](
      implicit keyEncoder: TupleEncoder[KeySchema],
      valueEncoder: TupleEncoder[ValueSchema],
      keyDecoder: TupleDecoder[KeySchema],
      valueDecoder: TupleDecoder[ValueSchema])
      extends TypedSubspace[Entity, Key] {

    def toKey(entity: Entity): Key
    def toKey(keyRepr: KeySchema): Key
    def toSubspaceKeyRepr(key: Key): KeySchema
    def toSubspaceValueRepr(entity: Entity): ValueSchema
    def toEntity(key: Key, valueRepr: ValueSchema): Entity

    final override def toRawValue(entity: Entity): Array[Byte] =
      valueEncoder.encode(toSubspaceValueRepr(entity)).pack()

    final override def toTupledKey(key: Key): Tuple =
      keyEncoder.encode(toSubspaceKeyRepr(key))

    final override def toKey(tupledKey: Tuple): Key =
      toKey(keyDecoder.decode(tupledKey))

    final override def toEntity(key: Key, value: Array[Byte]): Entity = {
      val valueRepr = valueDecoder.decode(Tuple.fromBytes(value))
      toEntity(key, valueRepr)
    }

    def clear[P <: Product, L <: HList](range: P)(
        implicit gen: Generic.Aux[P, L],
        enc: TupleEncoder[L],
        notKeyEv: L =:!= KeySchema,
        prefix: Prefix[KeySchema, L]): DBIO[Unit] = {
      this.clear(gen.to(range))
    }

    def clear[L <: HList](range: L)(
        implicit prefixEv: Prefix[KeySchema, L],
        enc: TupleEncoder[L],
        notKeyEv: L =:!= KeySchema): DBIO[Unit] = {
      super.clear(this.range(range))
    }

    def getRange[P <: Product, L <: HList](range: P)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: TupleEncoder[L]): ReadDBIO[Seq[Entity]] = {
      this.getRange(gen.to(range))
    }

    def getRange[L <: HList](range: L)(
        implicit prefixEv: Prefix[KeySchema, L],
        enc: TupleEncoder[L]): ReadDBIO[Seq[Entity]] = {
      super.getRange(this.range(range))
    }

    def getRange[P <: Product, L <: HList](range: P, limit: Int)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: TupleEncoder[L]): ReadDBIO[Seq[Entity]] = {
      this.getRange(gen.to(range), limit)
    }

    def getRange[L <: HList](range: L, limit: Int)(
        implicit prefixEv: Prefix[KeySchema, L],
        enc: TupleEncoder[L]): ReadDBIO[Seq[Entity]] = {
      super.getRange(this.range(range), limit)
    }

    def getRange[P <: Product, L <: HList](range: P, limit: Int, reverse: Boolean)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: TupleEncoder[L]): ReadDBIO[Seq[Entity]] = {
      this.getRange(gen.to(range), limit, reverse)
    }

    def getRange[L <: HList](range: L, limit: Int, reverse: Boolean)(
        implicit prefixEv: Prefix[KeySchema, L],
        enc: TupleEncoder[L]): ReadDBIO[Seq[Entity]] = {
      super.getRange(this.range(range), limit, reverse)
    }

    def range[P <: Product, L <: HList](range: P)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: TupleEncoder[L]): Range = {
      this.range(gen.to(range))
    }

    def range[L <: HList](
        range: L)(implicit prefixEv: Prefix[KeySchema, L], enc: TupleEncoder[L]): Range = {
      rangeFromHList(range)
    }

    final def firstGreaterOrEqual[P <: Product, L <: HList](key: P)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: TupleEncoder[L]): KeySelector =
      this.firstGreaterOrEqual(gen.to(key))

    final def firstGreaterOrEqual[L <: HList](
        key: L)(implicit prefixEv: Prefix[KeySchema, L], enc: TupleEncoder[L]): KeySelector =
      super.firstGreaterOrEqual(enc.encode(key))

    final def firstGreaterThan[P <: Product, L <: HList](key: P)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: TupleEncoder[L]): KeySelector =
      this.firstGreaterThan(gen.to(key))

    final def firstGreaterThan[L <: HList](
        key: L)(implicit prefixEv: Prefix[KeySchema, L], enc: TupleEncoder[L]): KeySelector =
      super.firstGreaterThan(enc.encode(key))

    final def lastLessOrEqual[P <: Product, L <: HList](key: P)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: TupleEncoder[L]): KeySelector =
      this.lastLessOrEqual(gen.to(key))

    final def lastLessOrEqual[L <: HList](
        key: L)(implicit prefixEv: Prefix[KeySchema, L], enc: TupleEncoder[L]): KeySelector =
      super.lastLessOrEqual(enc.encode(key))

    final def lastLessThan[P <: Product, L <: HList](key: P)(
        implicit gen: Generic.Aux[P, L],
        prefixEv: Prefix[KeySchema, L],
        enc: TupleEncoder[L]): KeySelector =
      this.lastLessThan(gen.to(key))

    final def lastLessThan[L <: HList](
        key: L)(implicit prefixEv: Prefix[KeySchema, L], enc: TupleEncoder[L]): KeySelector =
      super.lastLessThan(enc.encode(key))

    private def rangeFromHList[L <: HList](
        range: L)(implicit prefixEv: Prefix[KeySchema, L], enc: TupleEncoder[L]): Range = {
      val (tuple, trailingNulls) = enc.encode(range, new Tuple(), 0)
      // subspace.range adds additional \x00 at the end, so it doesn't select entry that was ending
      // with Option.empty (as those trailing nulls were not inserted),
      // that's why we need to start from subspace.pack here
      val start = subspace.pack(tuple)
      if (trailingNulls == 0) new Range(start, subspace.range(tuple).end)
      else {
        val endingTuple = EncodersHelpers.addNulls(tuple, trailingNulls)
        val end = subspace.range(endingTuple).end
        new Range(start, end)
      }
    }

  }

}
