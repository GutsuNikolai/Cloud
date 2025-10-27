// URL to fetch articles from
// Using a public placeholder API for demonstration
const URL = "https://jsonplaceholder.typicode.com/posts";

/**
 * Fetches articles from the specified URL and displays them on the webpage.
 * Each article includes a title, body, and user ID.
 * Handles errors by logging them to the console.
 */
function fetchArticles() {
  fetch(URL)
    .then((req) => req.json())
    .then((posts) => {
      const articlesContainer = document.querySelector("#posts");
      posts.forEach((post) => {
        const article = document.createElement("article");
        article.innerHTML = `
                <div class="post">
                    <h3>${post.title}</h3>
                    <p>${post.body}</p>
                    <i class="meta">User: ${post.userId}</i>
                </div>
            `;
        articlesContainer.appendChild(article);
      });
    })
    .catch((error) => console.error("Error fetching articles:", error));
}

fetchArticles();
