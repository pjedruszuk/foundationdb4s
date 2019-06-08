package com.github.pwliwanow.foundationdb4s.schema

import shapeless.{::, HList, HNil}

trait Prefix[L <: HList, Prefix <: HList] extends Serializable {
  def apply(x: L): Prefix
}

object Prefix extends PrefixDerivation

trait PrefixDerivation {

  implicit def hlistPrefixNil[L <: HList]: Prefix[L, HNil] = new Prefix[L, HNil] {
    override def apply(x: L): HNil = HNil
  }

  implicit def hlistPrefix[H, T <: HList, PH, PT <: HList](
      implicit equalsEv: H =:= PH,
      prefixEv: Prefix[T, PT]): Prefix[H :: T, PH :: PT] = new Prefix[H :: T, PH :: PT] {
    override def apply(x: H :: T): PH :: PT = {
      x match {
        case h :: t => equalsEv(h) :: prefixEv(t)
      }
    }
  }
}
