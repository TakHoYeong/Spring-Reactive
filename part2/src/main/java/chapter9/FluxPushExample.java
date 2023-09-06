package chapter9;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.util.List;

public class FluxPushExample {

    // 이벤트 리스너 인터페이스 정의
    interface SingleThreadEventListener<T> {
        void onDataChunk(List<T> chunk);
        void processComplete();
        void processError(Throwable e);
    }

    // 가상의 이벤트 프로세서
    static class MyEventProcessor {
        private SingleThreadEventListener<String> listener;

        public void register(SingleThreadEventListener<String> listener) {
            this.listener = listener;
        }

        // 이벤트를 시뮬레이션하는 메서드들
        public void simulateDataChunks() {
            listener.onDataChunk(List.of("data1", "data2", "data3"));
        }

        public void simulateCompletion() {
            listener.processComplete();
        }

        public void simulateError() {
            listener.processError(new RuntimeException("Some error occurred!"));
        }
    }

    public static void main(String[] args) {
        MyEventProcessor myEventProcessor = new MyEventProcessor();

        Flux<String> bridge = Flux.push(sink -> {
            myEventProcessor.register(new SingleThreadEventListener<String>() {

                public void onDataChunk(List<String> chunk) {
                    for (String s : chunk) {
                        sink.next(s);
                    }
                }

                public void processComplete() {
                    sink.complete();
                }

                public void processError(Throwable e) {
                    sink.error(e);
                }
            });
        });

        bridge.subscribe(
                data -> System.out.println("Received: " + data),
                err -> System.err.println("Error: " + err.getMessage()),
                () -> System.out.println("Stream completed")
        );

        // 시뮬레이션 메서드 호출
        myEventProcessor.simulateDataChunks();
        myEventProcessor.simulateCompletion();
    }
}
