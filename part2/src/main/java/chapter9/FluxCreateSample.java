package chapter9;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

public class FluxCreateSample {

    // Flux.create()는 Reactor의 Flux 클래스에서 제공하는 메소드 중 하나로, 개발자가 프로그래밍적으로 데이터를 방출할 수 있게 해줍니다.
    // Flux.create()는 FluxSink라는 콜백을 제공하여, 이를 통해 데이터 항목들을 방출하거나 에러를 전파하거나 시퀀스를 완료하는 등의 동작을 수행할 수 있습니다.
    //
    // 기본적인 사용법은 다음과 같습니다:
    //
    //   1. Flux.create()를 호출하면서 FluxSink를 처리하는 람다나 메소드 레퍼런스를 제공합니다.
    //   2. FluxSink의 next(), error(), complete() 메소드를 사용하여 이벤트를 방출합니다.
    public static void main(String[] args) {
        Flux<String> dynamicFlux = Flux.create(sink -> {
            sink.next("Hello");
            sink.next("World");
            sink.complete();
        });

        dynamicFlux.subscribe(System.out::println);
    }

    // 이 예제에서, Flux.create() 메소드를 사용하여 동적으로 문자열 "Hello"와 "World"를 방출하는 Flux를 생성하였습니다.
    // 그 후 subscribe() 메소드를 사용하여 데이터를 구독하고 출력하였습니다.
    //
    // Flux.create()는 다양한 실제 시나리오에서 유용하게 사용될 수 있습니다,
    // 예를 들면, 외부 이벤트 소스나 커스텀 데이터 소스에서 데이터를 방출할 때 등에 활용됩니다.
}
