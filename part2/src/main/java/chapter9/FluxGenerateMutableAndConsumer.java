package chapter9;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class FluxGenerateMutableAndConsumer {
    public static void main(String[] args) {

        AtomicLong finalState = new AtomicLong(0);

        Flux<String> flux = Flux.generate(
                // () -> new AtomicLong(),
                AtomicLong::new, // (1) Mutable State variant : state를 변경할 수 있는 객체를 state 값으로 생성
                (state, sink) -> {
                    long i = state.getAndIncrement(); // (2) 상태 변경
                    sink.next("3 x " + i + " = " + 3*i);
                    if (i == 10) sink.complete();
                    return state; // (3)
                },
                (state) -> System.out.println("state: " + state));

        flux.subscribe(data -> log.info("# onNext: {}", data), // 15:58:13.329 [main] INFO - # onNext: 3 x 10 = 30
                error -> log.info("# onError: {}", error.getMessage()),
                () -> log.info("# onComplete")); // 15:58:13.331 [main] INFO - # onComplete

        System.out.println("Final State: " + finalState.get());
    }
}
