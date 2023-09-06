package chapter9;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
public class FluxGenerate {

    // code로 Flux를 만드는 가장 쉬운 방법인 generate -> SynchronousSink
    // Reactor 프레임워크에서 "state"는 일반적으로 스트림 연산 도중 일시적으로 유지되는 상태나 데이터를 나타냅니다.
    // 이 상태는 연산의 내부 또는 다양한 연산 사이에서 정보를 유지하고 추적하는 데 사용될 수 있습니다.
    //
    // 특히, Flux.generate()나 Mono.create()와 같은 연산에서 "state"는 각각의 호출 또는 이벤트 사이에서 값을 유지하는 데 사용됩니다.
    // 예를 들어, 특정 연산의 반복적인 수행을 추적하거나, 이전 값에 따라 다음 값을 결정하는데 이 상태를 사용할 수 있습니다.
    //
    // 다음은 Flux.generate()의 예제에서 상태를 사용하는 방법입니다:
    //
    // Flux<String> flux = Flux.generate(
    //     () -> 0,  // 초기 상태 설정. 여기서는 정수 0입니다.
    //     (state, sink) -> {
    //         sink.next("data " + state);
    //         if (state >= 3) sink.complete();
    //         return state + 1;  // 상태 업데이트. 다음 호출에 사용될 새로운 상태를 반환합니다.
    //     }
    // );
    //
    // 위의 예제에서, 상태(state)는 정수이며, 각 `generate` 호출마다 1씩 증가합니다.
    //  state는 Flux.generate()의 연산 동안 값을 유지하며, 이를 통해 각 호출에서의 반복 횟수를 추적합니다.
    //
    // 이렇게 상태를 사용하면 함수형 프로그래밍 패러다임에서도 상태를 유지하고 추적할 수 있으며, 상태를 사용하여 연산의 흐름을 제어할 수 있습니다.


    // 아래의 generator처럼 동일하게 sink.next 메서드를 연속으로 호출할 수 없음.
    //    (state, sink) -> {
    //        sink.next("data1");
    //        sink.next("data2");  // 여기서 문제! 이미 next()를 호출한 후에 다시 next()를 호출했습니다.
    //        return state;
    //    }

    public static void main(String[] args) {
        Flux<String> flux = Flux.generate(
                () -> 0, // (1) state를 0으로 초기화한다
                (state, sink) -> {
                    log.info("# generator: state = {}", state);
                    sink.next("3 x " + state + " = " + 3*state); // (2) 데이터를 emit함.

                    if (state == 10) sink.complete(); // (3) 중단 시점을 체크한다.
                    return state + 1; // (4) 다음 실행에서 사용할 new state 값을 리턴한다.
                });

        flux.subscribe(data -> log.info("# onNext: {}", data), // 15:58:13.329 [main] INFO - # onNext: 3 x 10 = 30
                error -> log.info("# onError: {}", error.getMessage()),
                () -> log.info("# onComplete")); // 15:58:13.331 [main] INFO - # onComplete
    }
}
