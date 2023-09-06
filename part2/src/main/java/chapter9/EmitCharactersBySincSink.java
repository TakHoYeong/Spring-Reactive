package chapter9;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;

@Slf4j
public class EmitCharactersBySincSink {

    public static void main(String[] args) {
        CharacterGenerator characterGenerator = new CharacterGenerator();
        Flux<Character> characterFlux = characterGenerator.generateCharacters().take(3);

        characterFlux.subscribe(data -> log.info("data: {}", data));
    }
}
