package com.theaa;

import akka.NotUsed;
import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.IOResult;
import akka.stream.Materializer;
import akka.stream.ThrottleMode;
import akka.stream.javadsl.FileIO;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.util.ByteString;
import scala.concurrent.duration.Duration;

import java.math.BigInteger;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;

/**
 * Created by: fuzayl
 * Date: 21 Dec, 2018
 * Company: Finnet Limited
 */
public class SimpleStreamExpirements {
    public static void main(String[] args) {
        runStreamWithGraph();
    }

    private static void runStream() {
        ActorSystem system = ActorSystem.create();
        Materializer materializer = ActorMaterializer.create(system);

        Source.range(1, 100)
                .throttle(10,  Duration.apply(1, TimeUnit.SECONDS), 10, ThrottleMode.shaping())
                .mapAsync(10, el -> CompletableFuture.supplyAsync(() -> {
                    return el;
                }))
                .grouped(10)
                .runForeach(chunk -> System.out.println(chunk), materializer);
    }

    private static void writeToFile() {
        ActorSystem system = ActorSystem.create();
        Materializer materializer = ActorMaterializer.create(system);

        Source.range(1, 100)
                .throttle(10, Duration.apply(1, TimeUnit.SECONDS), 10, ThrottleMode.shaping())
                .scan(BigInteger.ONE, (acc, next) -> acc.multiply(BigInteger.valueOf(next)))
                .mapAsync(10, num -> CompletableFuture.supplyAsync(() -> {
                    return ByteString.fromString(num.toString() + "\n");
                }))
                .runWith(FileIO.toPath(Paths.get("factorials.txt")), materializer)
                .thenRun(() -> system.terminate());

    }

    private static void zipStreams() {
        ActorSystem system = ActorSystem.create();
        Materializer materializer = ActorMaterializer.create(system);

        Source.range(1, 100)
                .zipWith(Source.range(1, 100), (el1, el2) -> {
                    return el1 * el2;
                })
                .throttle(1,  Duration.apply(1, TimeUnit.SECONDS), 1, ThrottleMode.shaping())
                .runForeach(result -> System.out.println(result), materializer)
                .thenRun(() -> system.terminate());
    }

    private static void runStreamWithGraph() {
        ActorSystem system = ActorSystem.create();
        Materializer materializer = ActorMaterializer.create(system);

        final Source<ByteString, NotUsed> source =
                Source.from(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
                        .map(el -> {
                            return ByteString.fromString(el.toString() + "\n");
                        });

        final Sink<ByteString, CompletionStage<IOResult>> sink =
                FileIO.toPath(Paths.get("factorials2.txt"));

//        final RunnableGraph<CompletionStage<IOResult>> runnable =
//                source.toMat(sink, Keep.right());

//        final CompletionStage<IOResult> sum = runnable.run(materializer);

        final CompletionStage<IOResult> sum = source.runWith(sink, materializer);

        sum.thenRun(() -> system.terminate());
    }
}
