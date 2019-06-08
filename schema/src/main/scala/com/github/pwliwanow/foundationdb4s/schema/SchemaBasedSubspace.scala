package com.github.pwliwanow.foundationdb4s.schema

import com.apple.foundationdb.KeySelector
import com.apple.foundationdb.subspace.Subspace
import com.apple.foundationdb.tuple.Tuple
import com.apple.foundationdb.Range
import com.github.pwliwanow.foundationdb4s.core.{DBIO, ReadDBIO, TypedSubspace}
import shapeless.{=:!=, HList}

import scala.collection.immutable.Seq

abstract class SchemaBasedSubspace[Entity, Key, KeySchema <: HList, ValueSchema <: HList](
    implicit keyEncoder: TupleEncoder[KeySchema],
    valueEncoder: TupleEncoder[ValueSchema],
    keyDecoder: TupleDecoder[KeySchema],
    valueDecoder: TupleDecoder[ValueSchema])
    extends TypedSubspace[Entity, Key] {
  val subspace: Subspace

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

  def clear[L <: HList](range: L)(
      implicit prefixEv: Prefix[KeySchema, L],
      enc: TupleEncoder[L],
      notKeyEv: L =:!= KeySchema): DBIO[Unit] = {
    super.clear(this.range(range))
  }

  def getRange[L <: HList](range: L)(
      implicit prefixEv: Prefix[KeySchema, L],
      enc: TupleEncoder[L]): ReadDBIO[Seq[Entity]] = {
    super.getRange(this.range(range))
  }

  def getRange[L <: HList](range: L, limit: Int)(
      implicit prefixEv: Prefix[KeySchema, L],
      enc: TupleEncoder[L]): ReadDBIO[Seq[Entity]] = {
    super.getRange(this.range(range), limit)
  }

  def getRange[L <: HList](range: L, limit: Int, reverse: Boolean)(
      implicit prefixEv: Prefix[KeySchema, L],
      enc: TupleEncoder[L]): ReadDBIO[Seq[Entity]] = {
    super.getRange(this.range(range), limit, reverse)
  }

  def range[L <: HList](
      range: L)(implicit prefixEv: Prefix[KeySchema, L], enc: TupleEncoder[L]): Range = {
    rangeFromHList(range)
  }

  final def firstGreaterOrEqual[L <: HList](
      key: L)(implicit prefixEv: Prefix[KeySchema, L], enc: TupleEncoder[L]): KeySelector =
    super.firstGreaterOrEqual(enc.encode(key))

  final def firstGreaterThan[L <: HList](
      key: L)(implicit prefixEv: Prefix[KeySchema, L], enc: TupleEncoder[L]): KeySelector =
    super.firstGreaterThan(enc.encode(key))

  final def lastLessOrEqual[L <: HList](
      key: L)(implicit prefixEv: Prefix[KeySchema, L], enc: TupleEncoder[L]): KeySelector =
    super.lastLessOrEqual(enc.encode(key))

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
