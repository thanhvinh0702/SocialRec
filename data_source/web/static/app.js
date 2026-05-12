const state = {
  offset: 0,
  limit: 8,
  loading: false,
  hasMore: true,
  authenticated: false,
  username: document.body.dataset.username || "",
};

const feed = document.getElementById("feed");
const feedStatus = document.getElementById("feed-status");
const template = document.getElementById("post-template");
const signInPanel = document.getElementById("signed-in");
const signOutPanel = document.getElementById("signed-out");
const currentUsername = document.getElementById("current-username");
const usernameInput = document.getElementById("username-input");

function setAuthUi() {
  if (state.authenticated) {
    signInPanel.hidden = false;
    signOutPanel.hidden = true;
    currentUsername.textContent = state.username;
  } else {
    signInPanel.hidden = true;
    signOutPanel.hidden = false;
    currentUsername.textContent = "";
  }
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, {
    headers: { "Content-Type": "application/json" },
    ...options,
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || `request_failed_${response.status}`);
  }
  return response.json();
}

function splitTags(value) {
  if (!value) return [];
  return String(value)
    .split(/[,|\s]+/)
    .map((part) => part.trim())
    .filter((part) => part.length > 2)
    .slice(0, 8);
}

function renderComments(container, comments) {
  container.innerHTML = "";
  comments.forEach((comment) => {
    const el = document.createElement("div");
    el.className = "comment";
    el.innerHTML = `
      <strong>${comment.username}</strong>
      <span class="comment-meta">${comment.commented_at || ""}</span>
      <p>${comment.body_text}</p>
    `;
    container.appendChild(el);
  });
}

function renderPost(post) {
  const node = template.content.firstElementChild.cloneNode(true);
  node.dataset.postId = post.post_id;
  node.querySelector(".post-title").textContent = post.title;
  node.querySelector(".post-meta").textContent = `${post.author.username} · ${post.published_at || "unknown time"}`;
  node.querySelector(".post-body").textContent = post.excerpt || post.body_text || "";
  node.querySelector(".comment-count").textContent = `${post.comment_count} comments`;

  const link = node.querySelector(".post-link");
  if (post.canonical_url) {
    link.href = post.canonical_url;
  } else {
    link.removeAttribute("href");
    link.textContent = "";
  }

  const image = node.querySelector(".post-image");
  if (post.image_url) {
    image.src = post.image_url;
    image.style.display = "block";
  }

  const tagsEl = node.querySelector(".post-tags");
  [...splitTags(post.tags), ...splitTags(post.categories)].slice(0, 8).forEach((tag) => {
    const chip = document.createElement("span");
    chip.className = "tag";
    chip.textContent = tag;
    tagsEl.appendChild(chip);
  });

  const likeButton = node.querySelector(".like-button");
  likeButton.textContent = `Like · ${post.like_count}`;
  likeButton.classList.toggle("active", post.viewer_has_liked);
  likeButton.addEventListener("click", async () => {
    if (!state.authenticated) {
      alert("Sign in first.");
      return;
    }

    const payload = await requestJson(`/api/posts/${post.post_id}/like`, { method: "POST" });
    likeButton.textContent = `Like · ${payload.like_count}`;
    likeButton.classList.toggle("active", payload.liked);
  });

  const commentsEl = node.querySelector(".comment-list");
  renderComments(commentsEl, post.comments);

  const form = node.querySelector(".comment-form");
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!state.authenticated) {
      alert("Sign in first.");
      return;
    }

    const input = form.querySelector(".comment-input");
    const body_text = input.value.trim();
    if (!body_text) return;

    const payload = await requestJson(`/api/posts/${post.post_id}/comments`, {
      method: "POST",
      body: JSON.stringify({ body_text }),
    });

    input.value = "";
    post.comment_count += 1;
    node.querySelector(".comment-count").textContent = `${post.comment_count} comments`;
    post.comments.unshift(payload.comment);
    renderComments(commentsEl, post.comments.slice(0, 6));
  });

  return node;
}

async function loadPosts() {
  if (state.loading || !state.hasMore) return;
  state.loading = true;
  feedStatus.textContent = "Loading posts...";

  try {
    const payload = await requestJson(`/api/posts?offset=${state.offset}&limit=${state.limit}`);
    payload.posts.forEach((post) => feed.appendChild(renderPost(post)));
    state.offset = payload.next_offset;
    state.hasMore = payload.has_more;
    feedStatus.textContent = state.hasMore ? "Scroll for more" : "No more posts";
  } catch (error) {
    feedStatus.textContent = `Failed to load posts: ${error.message}`;
  } finally {
    state.loading = false;
  }
}

async function refreshAuthState() {
  const payload = await requestJson("/api/me");
  state.authenticated = payload.authenticated;
  state.username = payload.username || "";
  setAuthUi();
}

document.getElementById("signin-button").addEventListener("click", async () => {
  const username = usernameInput.value.trim();
  if (!username) return;

  await requestJson("/signin", {
    method: "POST",
    body: JSON.stringify({ username }),
  });

  feed.innerHTML = "";
  state.offset = 0;
  state.hasMore = true;
  await refreshAuthState();
  await loadPosts();
});

document.getElementById("signout-button").addEventListener("click", async () => {
  await fetch("/signout", { method: "POST" });
  feed.innerHTML = "";
  state.offset = 0;
  state.hasMore = true;
  await refreshAuthState();
  await loadPosts();
});

window.addEventListener("scroll", () => {
  if (window.innerHeight + window.scrollY >= document.body.offsetHeight - 800) {
    loadPosts();
  }
});

(async function bootstrap() {
  await refreshAuthState();
  await loadPosts();
})();
