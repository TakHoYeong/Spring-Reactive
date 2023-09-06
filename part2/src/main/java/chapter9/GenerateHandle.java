package chapter9;

import reactor.core.publisher.Flux;

public class GenerateHandle {

    public static String alphabet(int letterNumber) {
        if (letterNumber < 1 || letterNumber > 26) {
            return null;
        }
        int letterIndexAscii = 'A' + letterNumber - 1;
        return "" + (char) letterIndexAscii;
    }

    // Flux<R> handle(BiConsumer<T, SynchronousSink<R>>);
    // BiConsumer의 functional method : void accept(T t, U u);
    public static void main(String[] args) {
        Flux<String> alphabet = Flux.just(-1, 30, 13, 9, 20)
                .handle((i, sink) -> {
                    String letter = alphabet(i);
                    if (letter != null)
                        sink.next(letter);
                });

        alphabet.subscribe(System.out::println);
    }
}
