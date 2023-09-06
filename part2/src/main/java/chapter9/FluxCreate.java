package chapter9;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Slf4j
public class FluxCreate {

    // 리스너를 사용하여 외부 이벤트로부터 데이터를 Flux에 emit 하는 예제

    // FluxSink는 Reactor 프레임워크에서 제공하는 인터페이스로,
    // 데이터를 프로그래밍적으로 방출할 수 있는 방법을 제공합니다.
    // Flux.create()를 사용하여 Flux 인스턴스를 생성할 때, 개발자에게 FluxSink 객체가 전달되며,
    // 이를 통해 데이터 항목들을 방출하거나 에러를 전파하거나 시퀀스를 완료하는 등의 동작을 수행할 수 있습니다.
    //
    // FluxSink의 주요 메소드는 다음과 같습니다:
    //
    //  1. next(T value): 다음 데이터 항목을 방출합니다.
    //  2. error(Throwable e): 에러를 방출합니다. 이 메소드 호출 이후에는 다른 데이터나 시그널을 방출할 수 없습니다.
    //  3. complete(): 데이터 방출을 완료하고 시퀀스를 종료합니다. 이 메소드 호출 이후에는 다른 데이터나 시그널을 방출할 수 없습니다.
    //  4. onRequest(LongConsumer consumer): 구독자가 데이터를 요청할 때 호출되는 콜백을 설정합니다.
    //  5. onCancel(Disposable d): 구독이 취소되었을 때 호출되는 콜백을 설정합니다.
    //  6. onDispose(Disposable d): FluxSink가 해제될 때 호출되는 콜백을 설정합니다.


    // Flux.create() 메서드를 호출할 때,
    // Reactor 라이브러리 내부에서 FluxSink 객체가 자동으로 생성됩니다.
    // 이 객체는 개발자가 제공하는 람다 함수나 콜백에 전달되어, 개발자가 데이터를 방출하거나 에러를 전파하거나 시퀀스를 완료하는 동작을 수행할 수 있게 해줍니다.
    //
    // Flux.create()의 동작 원리를 간략하게 설명하면 다음과 같습니다:
    //
    //  1. Flux.create() 메서드가 호출되면, Reactor 라이브러리 내부에서 FluxSink의 구현체를 만듭니다.
    //  2. 해당 FluxSink 객체를 개발자가 제공한 람다 함수나 콜백에 전달합니다.
    //  3. 람다 함수나 콜백 내에서 FluxSink의 메소드를 호출하여 데이터를 방출하거나 다른 동작을 수행할 수 있습니다.
    //
    //
    //  Flux<String> flux = Flux.create(sink -> {
    //      sink.next("Hello");
    //      sink.next("World");
    //      sink.complete();
    //  });
    //
    // 위 코드에서 sink는 Reactor 라이브러리 내부에서 자동으로 생성된 FluxSink 객체입니다. 이 객체를 사용해서 람다 함수 내에서 데이터를 방출하거나 다른 동작을 수행할 수 있습니다.
    //
    // 요점은, 개발자가 직접 FluxSink 객체를 만들 필요는 없습니다. Flux.create() 메서드를 사용할 때, Reactor 라이브러리가 이를 자동으로 처리해줍니다.



    interface MyEventListener<T> {
        void onDataChunk(List<T> chunk);
        void processComplete();
    }

    static class MyEventProcessor {
        private List<MyEventListener<String>> listeners = new ArrayList<>();

        public void register(MyEventListener<String> listener) {
            listeners.add(listener);
        }

        // 이제 이벤트를 트리거할 때 listeners에 저장된 리스너들을 직접 호출합니다.
        public void triggerDataChunkEvent(List<String> chunk) {
            for (MyEventListener<String> listener : listeners) {
                listener.onDataChunk(chunk);
            }
        }

        public void triggerProcessCompleteEvent() {
            for (MyEventListener<String> listener : listeners) {
                listener.processComplete();
            }
        }
    }



    public static void main(String[] args) {
        FluxSink<String> fs;

        MyEventProcessor myEventProcessor = new MyEventProcessor();

        Flux<String> bridge = Flux.create(
                // Consumer<? super reactor.core.publisher.FluxSink<T>> emitter
                sink -> {

                    myEventProcessor.register(
                            new MyEventListener<String>() {
                                public void onDataChunk(List<String> chunk) {
                                    log.info("onDataChunk called");
                                    for (String s : chunk) {
                                        sink.next(s);
                                    }
                                }

                                public void processComplete() {
                                    sink.complete();
                                }
                            }
                     );
        });


        bridge.subscribe(data -> log.info("# onNext: {}", data), // 15:58:13.329 [main] INFO - # onNext: 3 x 10 = 30
                error -> log.info("# onError: {}", error.getMessage()),
                () -> log.info("# onComplete")); // 15:58:13.331 [main] INFO - # onComplete

        myEventProcessor.triggerDataChunkEvent(Arrays.asList("A", "B", "C", "D", "E", "F"));
        myEventProcessor.triggerDataChunkEvent(Arrays.asList("G", "H", "I", "J", "K", "L"));


        myEventProcessor.triggerProcessCompleteEvent();

    }
}
