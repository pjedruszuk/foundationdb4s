package com.github.pwliwanow.foundationdb4s.core

import java.util.concurrent.{CompletableFuture, CompletionException}

import cats.{Monad, StackSafeMonad}
import com.apple.foundationdb.{Transaction, TransactionContext}
import com.apple.foundationdb.tuple.Versionstamp

import scala.compat.java8.FutureConverters._
import scala.concurrent.{ExecutionContextExecutor, Future, Promise}

final case class DBIO[+A](
    private[foundationdb4s] val run: (Transaction, ExecutionContextExecutor) => Future[A]) {
  def map[B](f: A => B): DBIO[B] = DBIO {
    case (tx, ec) =>
      run(tx, ec).map(f)(ec)
  }

  def flatMap[B](f: A => DBIO[B]): DBIO[B] = DBIO {
    case (tx, ec) =>
      run(tx, ec).flatMap(a => f(a).run(tx, ec))(ec)
  }

  /** Runs a transactional DBIO with retry logic in a non-blocking way.
    *
    * In some cases client can be unable to determine whether a transaction succeeded.
    * In these cases, your transaction may be executed twice.
    * For more information see:
    * https://apple.github.io/foundationdb/developer-guide.html#transactions-with-unknown-results
    *
    * Any error encountered when executing DBIO will be set on the resulting Future.
    *
    * @return a [[Future]] that will contain a value returned by running this DBIO
    */
  def transact(transactor: Transactor): Future[A] = {
    transact(transactor.db)(transactor.ec)
  }

  /** Runs DBIO within provided [[TransactionContext]].
    *
    * Depending on the type of context, this may execute the supplied function multiple times
    * if an error is encountered.
    *
    * @return a [[Future]] that will contain a value returned by running this DBIO
    */
  def transact(context: TransactionContext)(implicit ec: ExecutionContextExecutor): Future[A] = {
    transactVersionstamped(context).map { case (x, _) => x }
  }

  /** Runs a transactional DBIO with retry logic in a non-blocking way.
    *
    * In some cases client can be unable to determine whether a transaction succeeded.
    * In these cases, your transaction may be executed twice.
    * For more information see:
    * https://apple.github.io/foundationdb/developer-guide.html#transactions-with-unknown-results
    *
    * Any error encountered when executing DBIO will be set on the resulting Future.
    *
    * @return a [[Future]] that will contain a tuple of value returned by running this DBIO and
    *         an optional [[Versionstamp]].
    *         Returned [[Versionstamp]] will be empty if executing this DBIO did not modify the database
    *         (e.g. when running this function on `DBIO.pure("value")`).
    *         For DBIO that modified the database, [[Versionstamp]] will be equal to the versionstamp used
    *         by any versionstamp operations in this DBIO.
    */
  def transactVersionstamped(
      transactor: Transactor,
      userVersion: Int): Future[(A, Option[Versionstamp])] = {
    transactVersionstamped(transactor.db, userVersion)(transactor.ec)
  }

  def transactVersionstamped(transactor: Transactor): Future[(A, Option[Versionstamp])] = {
    transactVersionstamped(transactor.db)(transactor.ec)
  }

  def transactVersionstamped(context: TransactionContext)(
      implicit ec: ExecutionContextExecutor): Future[(A, Option[Versionstamp])] = {
    transactVersionstamped(context, userVersion = 0)
  }

  /** Runs DBIO within provided [[TransactionContext]].
    *
    * Depending on the type of context, this may execute the supplied function multiple times
    * if an error is encountered.
    *
    * @return a [[Future]] that will contain a tuple of value returned by running this DBIO and
    *         an optional [[Versionstamp]].
    *         Returned [[Versionstamp]] will be empty if executing this DBIO did not modify the database
    *         (e.g. when running this function on `DBIO.pure("value")`).
    *         For DBIO that modified the database, [[Versionstamp]] will be equal to the versionstamp used
    *         by any versionstamp operations in this DBIO.
    */
  def transactVersionstamped(context: TransactionContext, userVersion: Int)(
      implicit ec: ExecutionContextExecutor): Future[(A, Option[Versionstamp])] = {
    val promisedVersionstamp = Promise[Option[Versionstamp]]
    val futureRes: Future[A] = context
      .runAsync(
        (tx: Transaction) => {
          val futureMaybeVersionstamp =
            tx.getVersionstamp.toScala
              .map { byteArray =>
                val versionstamp = Versionstamp.complete(byteArray, userVersion)
                Some(versionstamp)
              }
              .recover { case _ => None }
          promisedVersionstamp.completeWith(futureMaybeVersionstamp)
          run(tx, ec).toJava.asInstanceOf[CompletableFuture[A]]
        }
      )
      .toScala
    futureRes
      .zip(promisedVersionstamp.future)
      .recoverWith {
        case e: CompletionException if e.getCause != null =>
          Future.failed(e.getCause)
      }
  }
}

object DBIO {
  def failed[A](value: Throwable): DBIO[A] = DBIO[A] {
    case (_, _) => Future.failed[A](value)
  }

  def pure[A](value: A): DBIO[A] = DBIO {
    case (_, _) => Future.successful(value)
  }

  implicit val dbioMonad: Monad[DBIO] = new Monad[DBIO] with StackSafeMonad[DBIO] {
    override def pure[A](x: A): DBIO[A] = DBIO.pure(x)
    override def flatMap[A, B](fa: DBIO[A])(f: A => DBIO[B]): DBIO[B] = fa.flatMap(f)
  }
}
