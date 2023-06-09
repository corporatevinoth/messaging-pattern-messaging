package demo.microservices.messaging.patterns.claim.check.service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;

import reactor.core.publisher.Mono;

@Service
public class MessageService {
	
	private final WebClient webClient;
	
	public MessageService(WebClient.Builder webClientBuilder) {
		this.webClient=webClientBuilder.baseUrl("https://localhost:8093/storage").build();
	}
	
	private List<String> splitMessageIntoChunks(String message) {
		List<String> chunks = new ArrayList<>();
		int index = 0;
		int chunkSize = 100;
		
		while (index < message.length()) {
			chunks.add(message.substring(index, Math.min(index + chunkSize, message.length())));
			index += chunkSize;
		}
		
		return chunks;
	}
	@Async
	public CompletableFuture<List<String>> createMessage(String payload) {
		List<String> chunks = splitMessageIntoChunks(payload);
		List<String> claimIds = new ArrayList<>();
		
		for (String chunk: chunks) {

			claimIds.add(this.webClient
            .post()
            .uri("/receive")
            .contentType(MediaType.TEXT_PLAIN)
            .accept(MediaType.TEXT_PLAIN )
            .body(Mono.just(chunk),String.class)
            .retrieve()
            .bodyToMono(String.class).block());
		}
		
		return CompletableFuture.completedFuture(claimIds);
	}
	
	@Async
	public CompletableFuture<List<String>> getMessage(List<String> claimIds) {
		List<String> payload = new ArrayList<>();
		
		for (String claimId: claimIds) {
			payload.add(this.webClient.get()
					.uri("/get/"+claimId)
					.retrieve()
					.bodyToMono(String.class)
					.block());
		}
		
		
		return CompletableFuture.completedFuture(payload);
	}
}
