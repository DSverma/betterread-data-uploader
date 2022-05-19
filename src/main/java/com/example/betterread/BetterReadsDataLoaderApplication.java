package com.example.betterread;

import com.example.betterread.author.Author;
import com.example.betterread.author.AuthorRepository;
import com.example.betterread.book.Book;
import com.example.betterread.book.BookRepository;
import com.example.betterread.connection.DataStaxAstraProperties;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.cassandra.CqlSessionBuilderCustomizer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.chrono.ChronoLocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@SpringBootApplication
@EnableConfigurationProperties(DataStaxAstraProperties.class)
public class BetterReadsDataLoaderApplication {

	@Autowired AuthorRepository authorRepository;

	@Autowired
	BookRepository bookRepository;

	@Value("${datadump.location.author}")
	private String authorDumpLocation;


	@Value("${datadump.location.works}")
	private String workDumpLocation;

	public static void main(String[] args) {
		SpringApplication.run(BetterReadsDataLoaderApplication.class, args);
	}

	private void initAuthors(){
		Path path = Paths.get(authorDumpLocation);
		try(Stream<String> lines = Files.lines(path)){
			lines.skip(500315-1).forEach(line ->{
				//read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				JSONObject jsonObject = null;
				try {
					jsonObject = new JSONObject(jsonString);
					//construct Author object
					Author author = new Author();
					author.setName(jsonObject.optString("name"));
					author.setPersonalName(jsonObject.optString("personal_name"));
					author.setId(jsonObject.optString("key").replace("/authors/", ""));
					//persist into repository
					authorRepository.save(author);
					//System.out.println("Saving Author: "+author.getName());
				} catch (JSONException e) {
					e.printStackTrace();
				}

			});

		}catch (Exception exception){
			exception.printStackTrace();
		}

	}

	private void initWorks(){
		Path path = Paths.get(workDumpLocation);
		DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS");
		try(Stream<String> lines = Files.lines(path)){
			lines.limit(50).forEach(line ->{
				//read and parse the line
				String jsonString = line.substring(line.indexOf("{"));
				JSONObject jsonObject = null;
				try {
					jsonObject = new JSONObject(jsonString);
					//construct Book object
					Book book = new Book();
					book.setId(jsonObject.getString("key").replace("/works/",""));
					book.setName(jsonObject.optString("title"));

					JSONObject descriptionJsonObject1 = jsonObject.optJSONObject("description");
					if(descriptionJsonObject1 != null){
						book.setDescription(descriptionJsonObject1.optString("value"));
					}

					JSONObject publishedJsonObject1 = jsonObject.optJSONObject("created");
					if(publishedJsonObject1 != null){
						String dateStr = publishedJsonObject1.getString("value");
						book.setPublishedDate(LocalDate.parse(dateStr, dateFormat));
					}

					JSONArray coverIdsJsonArray = jsonObject.optJSONArray("covers");
					if(coverIdsJsonArray !=null){
						List<String> coverIds = new ArrayList<String>();
						for(int i=0; i < coverIdsJsonArray.length(); i++){
							coverIds.add(coverIdsJsonArray.getString(i));
						}
						book.setCoverIds(coverIds);
					}

					JSONArray authorsJsonArray = jsonObject.optJSONArray("authors");
					if(authorsJsonArray !=null){
						List<String> authors = new ArrayList<String>();
						for(int i=0; i < authorsJsonArray.length(); i++){
							String authorId = authorsJsonArray.getJSONObject(i).getJSONObject("author")
									.getString("key").replace("/authors/", "");
							authors.add(authorId);
						}
						book.setAuthorIds(authors);

						List<String> authorNames = authors.stream().map(id -> authorRepository.findById(id))
								.map(optionalAuthor -> {
									if (!optionalAuthor.isPresent()) {
										return "Unknown Author";
									} else {
										return optionalAuthor.get().getName();
									}
								}).collect(Collectors.toList());
						book.setAuthorNames(authorNames);
					}

					//persist into repository
					System.out.println("Saving book: "+book.getName());
					bookRepository.save(book);
					//System.out.println("Saving Author: "+author.getName());
				} catch (Exception e) {
					e.printStackTrace();
				}

			});

		}catch (IOException exception){
			exception.printStackTrace();
		}

	}


	@PostConstruct
	public void start(){

		System.out.println("***** Authors ******* "+authorDumpLocation);
		initAuthors();

//		System.out.println("****** Books ****** "+workDumpLocation);
//		initWorks();

	}




	@Bean
	public CqlSessionBuilderCustomizer sessionBuilderCustomizer(DataStaxAstraProperties astraProperties){
		Path bundle = astraProperties.getSecureConnectBundle().toPath();
		return builder -> builder.withCloudSecureConnectBundle(bundle);
	}

}
