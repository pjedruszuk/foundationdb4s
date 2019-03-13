package com.github.pwliwanow.foundationdb4s.core

import java.util.concurrent.{CompletableFuture, Executor}
import java.util.function

import cats.kernel.laws.IsEq
import cats.laws.MonadLaws
import com.apple.foundationdb.{ReadTransaction, Transaction, TransactionContext}
import com.apple.foundationdb.tuple.Tuple
import com.github.pwliwanow.foundationdb4s.core.Pet.{Cat, Dog}

import scala.util.Try

class ReadDBIOSpec extends FoundationDbSpec {

  private val monadLaws = MonadLaws[ReadDBIO]

  it should "satisfy monadLeftIdentity" in {
    val table =
      Table(
        ("element", "element to ReadDBIO"),
        (1, (x: Int) => ReadDBIO.pure(x.toString)),
        (2, (_: Int) => ReadDBIO.failed[String](TestError("Error"))))
    forAll(table) { (x: Int, f: Int => ReadDBIO[String]) =>
      val isEq = monadLaws.monadLeftIdentity(x, f)
      assertReadDbioEq(isEq)
    }
  }

  it should "satisfy monadRightIdentity" in {
    val table =
      Table("fa", ReadDBIO.pure("Success"), ReadDBIO.failed[String](TestError("Error")))
    forAll(table) { dbio: ReadDBIO[String] =>
      val isEq = monadLaws.monadRightIdentity(dbio)
      assertReadDbioEq(isEq)
    }
  }

  it should "satisfy kleisliLeftIdentity" in {
    val table =
      Table(
        ("element", "element to ReadDBIO"),
        (1, (x: Int) => ReadDBIO.pure(x.toString)),
        (2, (_: Int) => ReadDBIO.failed[String](TestError("Error"))))
    forAll(table) { (x: Int, f: Int => ReadDBIO[String]) =>
      val isEq = monadLaws.kleisliLeftIdentity(x, f)
      assertReadDbioEq(isEq)
    }
  }

  it should "satisfy kleisliRightIdentity" in {
    val table =
      Table(
        ("element", "element to ReadDBIO"),
        (1, (x: Int) => ReadDBIO.pure(x.toString)),
        (2, (_: Int) => ReadDBIO.failed[String](TestError("Error"))))
    forAll(table) { (x: Int, f: Int => ReadDBIO[String]) =>
      val isEq = monadLaws.kleisliRightIdentity(x, f)
      assertReadDbioEq(isEq)
    }
  }

  it should "satisfy mapFlatMapCoherence" in {
    val table =
      Table(
        ("ReadDBIO", "f: A => B"),
        (ReadDBIO.pure(1), (x: Int) => x.toString),
        (ReadDBIO.pure(2), (_: Int) => throw TestError("map error")),
        (ReadDBIO.failed[Int](TestError("failed 3")), (x: Int) => x.toString),
        (ReadDBIO.failed[Int](TestError("failed 4")), (_: Int) => throw TestError("map 4 error"))
      )
    forAll(table) { (x: ReadDBIO[Int], f: Int => String) =>
      val isEq = monadLaws.mapFlatMapCoherence(x, f)
      assertReadDbioEq(isEq)
    }
  }

  it should "satisfy tailRecMStackSafety" in {
    val isEq = monadLaws.tailRecMStackSafety
    assertReadDbioEq(isEq)
  }

  it should "not fail with stack overflow for deeply nested combination of TryAction and FlatMaps" in {
    val action = (_: ReadTransaction) => Try("some value")
    val n = 50000
    val deeplyNested = (1 to n).foldLeft(ReadDBIO.pure("")) { (dbio, _) =>
      dbio.flatMap(_ => ReadDBIO.fromTransactionToTry(action))
    }
    deeplyNested.transact(contextWithNullTransaction)
  }

  it should "not fail with stack overflow for deeply nested combination of FutureAction and FlatMaps" in {
    val action = (_: ReadTransaction) => CompletableFuture.supplyAsync[String](() => "some value")
    val n = 50000
    val deeplyNested = (1 to n).foldLeft(ReadDBIO.pure("")) { (dbio, _) =>
      dbio.flatMap(_ => ReadDBIO.fromTransactionToPromise(action))
    }
    deeplyNested.transact(contextWithNullTransaction)
  }

  it should "be able to convert itself to DBIO" in {
    val lhs = ReadDBIO.pure(10).toDBIO.flatMap(x => DBIO.pure(x.toString))
    val rhs = DBIO.pure("10")
    assertDbioEq(IsEq(lhs, rhs))
  }

  it should "be converted to DBIO correctly" in {
    val table =
      Table(
        ("ReadDBIO", "Expected DBIO"),
        (ReadDBIO.pure(10), DBIO.pure(10)),
        (ReadDBIO.failed[Int](TestError("Failed")), DBIO.failed[Int](TestError("Failed")))
      )
    forAll(table) { (readDbio: ReadDBIO[Int], dbio: DBIO[Int]) =>
      val lhs = readDbio.toDBIO
      val rhs = dbio
      assertDbioEq(IsEq(lhs, rhs))
    }
  }

  it should "be able to read values" in {
    val key = subspace.pack(Tuple.from("testKey"))
    val value = Tuple.from("value").pack
    database.run(tx => tx.set(key, value))
    val readDbio = ReadDBIO.fromTransactionToPromise(tx => tx.get(key))
    val _ = readDbio.transact(database).await
    val result = {
      val byteArray = readDbio.transact(database).await
      Tuple.fromBytes(byteArray).getString(0)
    }
    assert(result === "value")
  }

  it should "correctly handle covariance" in {
    import cats.implicits._
    val cat = Cat("Tom")
    val dog = Dog("Rico")
    val expected = List(cat, dog)
    val catReadDbio: ReadDBIO[Cat] =
      ReadDBIO.fromTransactionToPromise(_ => CompletableFuture.supplyAsync(() => cat))
    val dogReadDbio: ReadDBIO[Dog] =
      ReadDBIO.fromTransactionToTry(_ => Try(dog))
    val dbioPets: ReadDBIO[List[Pet]] = List(catReadDbio, dogReadDbio).sequence
    val received = dbioPets.transact(database).await
    assert(received === expected)
  }

  private def contextWithNullTransaction: TransactionContext = new TransactionContext {
    override def run[T](retryable: function.Function[_ >: Transaction, T]): T = {
      retryable(null)
    }

    override def runAsync[T](
        retryable: function.Function[_ >: Transaction, _ <: CompletableFuture[T]])
      : CompletableFuture[T] = {
      retryable(null)
    }

    override def read[T](retryable: function.Function[_ >: ReadTransaction, T]): T = {
      retryable(null)
    }

    override def readAsync[T](
        retryable: function.Function[_ >: ReadTransaction, _ <: CompletableFuture[T]])
      : CompletableFuture[T] = {
      retryable(null)
    }

    override def getExecutor: Executor = ec
  }

}
