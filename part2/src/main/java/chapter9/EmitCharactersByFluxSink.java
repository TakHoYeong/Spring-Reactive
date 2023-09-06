package chapter9;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class EmitCharactersByFluxSink {

    public static void main(String[] args) {
        CharacterGenerator characterGenerator = new CharacterGenerator();
        List<Character> sequence1 = characterGenerator.generateCharacters().take(3).collectList().block();
        List<Character> sequence2 = characterGenerator.generateCharacters().take(2).collectList().block();

        log.info("sequence1: {}", sequence1);
        log.info("sequence2: {}", sequence2);

        CharacterCreator characterCreator = new CharacterCreator();
        Thread producerThread1 = new Thread(() -> characterCreator.consumer.accept(sequence1));
        Thread producerThread2 = new Thread(() -> characterCreator.consumer.accept(sequence2));

        List<Character> consolidated = new ArrayList<>();
        characterCreator.createCharacterSequence().subscribe(consolidated::add);

        try {
            producerThread1.start();
            producerThread2.start();
            producerThread1.join();
            producerThread2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        log.info("consolidated: {}", consolidated);

    }
}
