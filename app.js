/**
 * NEURA-CORE v5.0 - Enterprise Monolithic Engine
 * Features:
 * - Framework-less Native Web Components with Shadow DOM Encapsulation
 * - Context-Aware Message History Engine (Context Store)
 * - Native Web Streams (ReadableStream API) - Worker-less Token Generator
 * - Async Microtask Loop & Event Driven Observer Pattern
 * - Complete Error Boundary & Exception Handling Engine
 * - Hardware Accelerated Canvas Neural Node Visualizer
 */

// ==========================================================================
// 1. SYSTEM UTILITIES & TOAST ENGINE
// ==========================================================================

class CyberToastService {
  constructor(containerId) {
    this.container = document.getElementById(containerId);
  }

  show(message, type = 'info', duration = 4000) {
    if (!this.container) return;

    const toast = document.createElement('div');
    toast.className = `cyber-toast ${type}`;
    
    const icon = type === 'success' ? '✓' : type === 'danger' ? '⚠' : 'ℹ';
    toast.innerHTML = `<span>${icon}</span> <span>${message}</span>`;

    this.container.appendChild(toast);

    setTimeout(() => {
      toast.style.opacity = '0';
      toast.style.transform = 'translateX(100px)';
      setTimeout(() => toast.remove(), 300);
    }, duration);
  }
}

const toastService = new CyberToastService('system-toast-container');

// ==========================================================================
// 2. CONTEXT-AWARE STATE STORE ENGINE (OBSERVER PATTERN)
// ==========================================================================

class AIContextStore {
  constructor() {
    this.state = {
      messages: [], // [{ id, role, content, timestamp, tokenLength }]
      isStreaming: false,
      metrics: {
        totalTokensGenerated: 0,
        requestLatencyMs: 0,
        totalRequests: 0,
        lastActiveTimestamp: null
      },
      currentStreamBuffer: "",
      systemStatus: "IDLE" // "IDLE" | "PROCESSING" | "ERROR"
    };

    this.listeners = new Set();
  }

  subscribe(listener) {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  notify() {
    for (const listener of this.listeners) {
      try {
        listener(this.state);
      } catch (err) {
        console.error("[NEURA-STORE ERROR]: Observer update failed", err);
      }
    }
  }

  getState() {
    return this.state;
  }

  addMessage(role, content) {
    if (!content || typeof content !== 'string') {
      throw new Error("[STORE ERROR]: Invalid message content payload");
    }

    const messageObj = {
      id: crypto.randomUUID(),
      role,
      content: content.trim(),
      timestamp: new Date().toLocaleTimeString(),
      tokenLength: content.split(/\s+/).length
    };

    this.state.messages.push(messageObj);
    this.notify();
    return messageObj;
  }

  startStreamingSession() {
    this.state.isStreaming = true;
    this.state.currentStreamBuffer = "";
    this.state.systemStatus = "PROCESSING";
    this.state.metrics.totalRequests++;
    this.state.metrics.lastActiveTimestamp = Date.now();
    this.notify();
  }

  appendStreamChunk(chunk) {
    if (!this.state.isStreaming) return;
    this.state.currentStreamBuffer += chunk;
    this.state.metrics.totalTokensGenerated++;
    this.notify();
  }

  finalizeStreamingSession() {
    if (this.state.currentStreamBuffer.trim()) {
      this.addMessage('assistant', this.state.currentStreamBuffer);
    }
    this.state.currentStreamBuffer = "";
    this.state.isStreaming = false;
    this.state.systemStatus = "IDLE";
    this.notify();
  }

  setSystemError(errorMessage) {
    this.state.isStreaming = false;
    this.state.currentStreamBuffer = "";
    this.state.systemStatus = "ERROR";
    this.notify();
    toastService.show(errorMessage, 'danger', 5000);
  }

  getContextHistory(limit = 12) {
    // Return last N items to maintain strict LLM Context Window
    return this.state.messages.slice(-limit);
  }

  clearHistory() {
    this.state.messages = [];
    this.state.metrics.totalTokensGenerated = 0;
    this.notify();
    toastService.show("Xotira va kontekst tozalandi.", "info");
  }
}

const globalStore = new AIContextStore();

// ==========================================================================
// 3. WORKER-LESS AI STREAMING ENGINE (NATIVE READABLESTREAM & ASYNC LOOPS)
// ==========================================================================

class AIStreamingEngine {
  constructor() {
    this.vocabulary = [
      "AI", "Neyron", "Kogerentlik", "Kvant", "Algoritm", "Kontekst", "Xotira", 
      "Vektor", "Transformator", "Semantik", "Tahlil", "Optimizatsiya", "Sinaps",
      "Matritsa", "Klaster", "Oqim", "Modul", "Protokol", "Jarayon", "Resurs",
      "Kogerent", "Arxitektura", "Sinxronitatsiya", "Inference", "Tensor"
    ];

    this.knowledgeBase = {
      greeting: "Salom! Men NEURA-CORE v5.0 AI assistentiman. Kontekstni eslab qolish va real-vaqtli streaming orqali savollaringizga javob berishga tayyorman.",
      architecture: "Ushbu loyiha arxitekturasi worker-less formatda yozilgan: 1 ta HTML, 1 ta CSS va 1 ta JS. Barcha asinxron hisoblashlar Native Web Streams va Microtask Loop orqali boshqariladi.",
      context: "Kontekst xotirasi to'liq ishlamoqda. Men siz ko'rsatgan va oldingi suhbatlarda aytilgan ma'lumotlarni eslab qolaman.",
      capabilities: "Men quyidagi imkoniyatlarga egaman: Context-awareness, real-time token streaming, GPU Canvas animatsiyasi va Shadow DOM orqali to'liq izolyatsiya qilingan UI.",
      defaultResponse: "Tizim so'rovingizni muvaffaqiyatli tahlil qildi. Natijaviy neyron matritsa javobi shakllantirildi: "
    };
  }

  async processPromptStream(prompt, contextHistory, onChunk, onComplete, onError) {
    if (!prompt || typeof prompt !== 'string') {
      onError(new Error("Yaroqsiz so'rov kiritildi."));
      return;
    }

    try {
      const fullResponseText = this.synthesizeTextResponse(prompt, contextHistory);
      const stream = this.createReadableStream(fullResponseText);
      const reader = stream.getReader();

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        onChunk(value);
      }

      onComplete();
    } catch (err) {
      onError(err);
    }
  }

  synthesizeTextResponse(prompt, contextHistory) {
    const p = prompt.toLowerCase().trim();

    if (p.includes("salom") || p.includes("kim")) {
      return this.knowledgeBase.greeting;
    }
    if (p.includes("arxitektura") || p.includes("tuzilish") || p.includes("kod")) {
      return this.knowledgeBase.architecture;
    }
    if (p.includes("kontekst") || p.includes("xotira")) {
      return `${this.knowledgeBase.context} Hozirda faol xotira bazasida ${contextHistory.length} ta xabar mavjud.`;
    }
    if (p.includes("imkoniyat") || p.includes("nima qila olasan")) {
      return this.knowledgeBase.capabilities;
    }

    // Dynamic Artificial Content Engine
    let generatedWords = [];
    const wordLength = Math.floor(Math.random() * 40) + 25;
    for (let i = 0; i < wordLength; i++) {
      const randomWord = this.vocabulary[Math.floor(Math.random() * this.vocabulary.length)];
      generatedWords.push(randomWord);
    }

    return `${this.knowledgeBase.defaultResponse} "${prompt}" -> ` + generatedWords.join(" ") + ".";
  }

  createReadableStream(text) {
    const words = text.split(" ");
    let index = 0;

    return new ReadableStream({
      async pull(controller) {
        if (index < words.length) {
          // Dynamic streaming latency simulation
          const delay = Math.floor(Math.random() * 30) + 20;
          await new Promise(resolve => setTimeout(resolve, delay));

          const chunk = words[index] + " ";
          controller.enqueue(chunk);
          index++;
        } else {
          controller.close();
        }
      }
    });
  }
}

const aiStreamingEngine = new AIStreamingEngine();

// ==========================================================================
// 4. GPU ACCELERATED BACKGROUND NEURAL CANVAS ENGINE
// ==========================================================================

class CyberBackgroundCanvas {
  constructor(canvasId) {
    this.canvas = document.getElementById(canvasId);
    if (!this.canvas) return;

    this.ctx = this.canvas.getContext('2d');
    this.particles = [];
    this.particleCount = 70;
    this.animationFrameId = null;

    this.init();
  }

  init() {
    this.handleResize();
    this.spawnParticles();
    window.addEventListener('resize', () => this.handleResize());
    this.startRenderLoop();
  }

  handleResize() {
    this.canvas.width = window.innerWidth;
    this.canvas.height = window.innerHeight;
  }

  spawnParticles() {
    this.particles = [];
    for (let i = 0; i < this.particleCount; i++) {
      this.particles.push({
        x: Math.random() * this.canvas.width,
        y: Math.random() * this.canvas.height,
        vx: (Math.random() - 0.5) * 1.1,
        vy: (Math.random() - 0.5) * 1.1,
        radius: Math.random() * 2 + 1,
        color: Math.random() > 0.5 ? '#00f2fe' : '#7000ff'
      });
    }
  }

  startRenderLoop() {
    this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);

    for (let i = 0; i < this.particles.length; i++) {
      let p = this.particles[i];
      p.x += p.vx;
      p.y += p.vy;

      if (p.x < 0 || p.x > this.canvas.width) p.vx *= -1;
      if (p.y < 0 || p.y > this.canvas.height) p.vy *= -1;

      this.ctx.beginPath();
      this.ctx.arc(p.x, p.y, p.radius, 0, Math.PI * 2);
      this.ctx.fillStyle = p.color;
      this.ctx.shadowBlur = 10;
      this.ctx.shadowColor = p.color;
      this.ctx.fill();

      // Neural Connectors
      for (let j = i + 1; j < this.particles.length; j++) {
        let p2 = this.particles[j];
        let dist = Math.hypot(p2.x - p.x, p2.y - p.y);
        if (dist < 130) {
          this.ctx.beginPath();
          this.ctx.moveTo(p.x, p.y);
          this.ctx.lineTo(p2.x, p2.y);
          this.ctx.strokeStyle = `rgba(0, 242, 254, ${1 - dist / 130})`;
          this.ctx.lineWidth = 0.5;
          this.ctx.stroke();
        }
      }
    }

    this.animationFrameId = requestAnimationFrame(() => this.startRenderLoop());
  }

  destroy() {
    if (this.animationFrameId) {
      cancelAnimationFrame(this.animationFrameId);
    }
  }
}

// ==========================================================================
// 5. WEB COMPONENT ENGINE: <ai-agent-interface>
// ==========================================================================

class AIAgentInterface extends HTMLElement {
  constructor() {
    super();
    this.attachShadow({ mode: 'open' });
    this.unsubscribeStore = null;
    this.requestStartTime = 0;
  }

  connectedCallback() {
    this.renderDOMStructure();
    this.injectShadowStyles();
    this.attachEventListeners();

    // Subscribe to state updates
    this.unsubscribeStore = globalStore.subscribe((state) => this.onStateUpdated(state));
    toastService.show("NEURA-CORE v5.0 AI Agent tayyor.", "success");
  }

  disconnectedCallback() {
    if (this.unsubscribeStore) {
      this.unsubscribeStore();
    }
  }

  renderDOMStructure() {
    this.shadowRoot.innerHTML = `
      <div class="cyber-card-wrapper">
        <!-- Header Controls & Telemetry Bar -->
        <header class="card-header">
          <div class="brand-badge">
            <div class="status-pulse-dot"></div>
            <h2>NEURA<span>-CORE</span> AI Agent</h2>
          </div>
          <div class="telemetry-bar">
            <div class="metric-item">TOKENS: <span id="m-tokens">0</span></div>
            <div class="metric-item">LATENCY: <span id="m-latency">0ms</span></div>
            <div class="metric-item">CONTEXT: <span id="m-context">0 msgs</span></div>
            <button id="btn-diag" class="icon-btn" title="Tizim diagnostikasi">⚙</button>
            <button id="btn-clear" class="icon-btn danger" title="Xotirani tozalash">🗑</button>
          </div>
        </header>

        <!-- Dynamic Chat Stream Display -->
        <section class="chat-viewport" id="chat-viewport" aria-live="polite">
          <div class="hero-welcome-box">
            <div class="hero-icon">⚡</div>
            <h3>MONOLITHIC AI AGENT ENGINE ONLINE</h3>
            <p>Xohlagan savolingizni bering. Tizim javobni real-vaqtda stream ko'rinishida beradi.</p>
          </div>
        </section>

        <!-- Prompt Input Dock -->
        <footer class="card-footer">
          <form id="ai-prompt-form" class="prompt-form-grid">
            <textarea id="user-prompt-input" placeholder="Surovingizni yozing (masalan: 'Arxitektura haqida gapir' yoki 'Kontekst nimadan iborat?')..." rows="1" required></textarea>
            <button type="submit" id="btn-submit-prompt" class="cyber-action-button">
              <span>YUBORISH</span>
            </button>
          </form>
        </footer>
      </div>
    `;
  }

  injectShadowStyles() {
    const styleTag = document.createElement('style');
    styleTag.textContent = `
      :host {
        display: block;
        width: 100%;
        height: 100%;
      }

      .cyber-card-wrapper {
        display: flex;
        flex-direction: column;
        height: 100%;
        background: rgba(11, 19, 43, 0.75);
        backdrop-filter: blur(25px);
        -webkit-backdrop-filter: blur(25px);
        border: 1px solid rgba(0, 242, 254, 0.2);
        border-radius: 20px;
        box-shadow: 0 20px 50px rgba(0, 0, 0, 0.8), inset 0 0 15px rgba(0, 242, 254, 0.1);
        overflow: hidden;
      }

      .card-header {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding: 20px 28px;
        background: rgba(3, 7, 18, 0.6);
        border-bottom: 1px solid rgba(0, 242, 254, 0.15);
      }

      .brand-badge {
        display: flex;
        align-items: center;
        gap: 12px;
      }

      .status-pulse-dot {
        width: 12px;
        height: 12px;
        border-radius: 50%;
        background: #00ff88;
        box-shadow: 0 0 10px #00ff88;
      }

      .brand-badge h2 {
        font-family: 'Fira Code', monospace;
        font-size: 1.25rem;
        color: #fff;
        letter-spacing: 1.5px;
      }

      .brand-badge h2 span { color: #00f2fe; }

      .telemetry-bar {
        display: flex;
        align-items: center;
        gap: 18px;
        font-family: 'Fira Code', monospace;
        font-size: 0.85rem;
        color: #94a3b8;
      }

      .metric-item span { color: #00f2fe; font-weight: bold; }

      .icon-btn {
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid rgba(255, 255, 255, 0.1);
        color: #fff;
        padding: 6px 10px;
        border-radius: 8px;
        cursor: pointer;
        transition: all 0.2s ease;
      }

      .icon-btn:hover { background: rgba(0, 242, 254, 0.2); border-color: #00f2fe; }
      .icon-btn.danger:hover { background: rgba(255, 0, 85, 0.2); border-color: #ff0055; }

      .chat-viewport {
        flex: 1;
        overflow-y: auto;
        padding: 28px;
        display: flex;
        flex-direction: column;
        gap: 18px;
      }

      .hero-welcome-box {
        text-align: center;
        padding: 40px;
        border: 1px dashed rgba(0, 242, 254, 0.2);
        border-radius: 16px;
        background: rgba(0, 242, 254, 0.02);
      }

      .hero-icon { font-size: 2.5rem; margin-bottom: 12px; }
      .hero-welcome-box h3 { color: #00f2fe; margin-bottom: 8px; font-family: 'Fira Code', monospace; }
      .hero-welcome-box p { color: #94a3b8; font-size: 0.95rem; }

      .message-row {
        display: flex;
        flex-direction: column;
        max-width: 82%;
        animation: streamIn 0.25s ease-out forwards;
      }

      .message-row.user { align-self: flex-end; }
      .message-row.assistant { align-self: flex-start; }

      .msg-header {
        font-size: 0.75rem;
        font-family: 'Fira Code', monospace;
        color: #64748b;
        margin-bottom: 4px;
      }

      .msg-bubble {
        padding: 14px 18px;
        border-radius: 14px;
        font-size: 0.95rem;
        line-height: 1.6;
        word-break: break-word;
      }

      .user .msg-bubble {
        background: linear-gradient(135deg, #00f2fe, #4facfe);
        color: #000;
        font-weight: 500;
        border-bottom-right-radius: 2px;
      }

      .assistant .msg-bubble {
        background: rgba(15, 23, 42, 0.85);
        border: 1px solid rgba(0, 242, 254, 0.2);
        color: #f8fafc;
        border-bottom-left-radius: 2px;
        box-shadow: inset 0 0 10px rgba(0, 242, 254, 0.05);
      }

      .streaming-cursor::after {
        content: '▋';
        color: #00f2fe;
        animation: cursorBlink 0.8s infinite;
        margin-left: 4px;
      }

      .card-footer {
        padding: 20px 28px;
        background: rgba(3, 7, 18, 0.6);
        border-top: 1px solid rgba(0, 242, 254, 0.15);
      }

      .prompt-form-grid {
        display: flex;
        gap: 16px;
      }

      textarea {
        flex: 1;
        background: rgba(15, 23, 42, 0.9);
        border: 1px solid rgba(0, 242, 254, 0.2);
        border-radius: 12px;
        padding: 14px 18px;
        color: #fff;
        font-family: inherit;
        font-size: 0.95rem;
        resize: none;
        outline: none;
        transition: border 0.2s ease, box-shadow 0.2s ease;
      }

      textarea:focus { border-color: #00f2fe; box-shadow: 0 0 15px rgba(0, 242, 254, 0.2); }

      .cyber-action-button {
        background: linear-gradient(135deg, #00f2fe, #7000ff);
        border: none;
        border-radius: 12px;
        padding: 0 28px;
        color: #fff;
        font-family: 'Fira Code', monospace;
        font-weight: bold;
        cursor: pointer;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
      }

      .cyber-action-button:hover:not(:disabled) {
        transform: translateY(-2px);
        box-shadow: 0 0 20px rgba(0, 242, 254, 0.4);
      }

      .cyber-action-button:disabled {
        opacity: 0.5;
        cursor: not-allowed;
      }
    `;
    this.shadowRoot.appendChild(styleTag);
  }

  attachEventListeners() {
    const form = this.shadowRoot.getElementById('ai-prompt-form');
    const textarea = this.shadowRoot.getElementById('user-prompt-input');
    const btnClear = this.shadowRoot.getElementById('btn-clear');
    const btnDiag = this.shadowRoot.getElementById('btn-diag');

    textarea.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        form.requestSubmit();
      }
    });

    form.addEventListener('submit', (e) => {
      e.preventDefault();
      const prompt = textarea.value.trim();
      if (!prompt || globalStore.getState().isStreaming) return;

      textarea.value = '';
      this.handleUserPromptSubmission(prompt);
    });

    btnClear.addEventListener('click', () => globalStore.clearHistory());
    btnDiag.addEventListener('click', () => {
      const modal = document.getElementById('system-diagnostics-modal');
      if (modal) modal.showModal();
    });
  }

  async handleUserPromptSubmission(prompt) {
    try {
      globalStore.addMessage('user', prompt);
      globalStore.startStreamingSession();

      this.requestStartTime = performance.now();

      await aiStreamingEngine.processPromptStream(
        prompt,
        globalStore.getContextHistory(),
        (chunk) => {
          globalStore.appendStreamChunk(chunk);
        },
        () => {
          const latency = Math.round(performance.now() - this.requestStartTime);
          this.shadowRoot.getElementById('m-latency').textContent = `${latency}ms`;
          globalStore.finalizeStreamingSession();
        },
        (err) => {
          globalStore.setSystemError(err.message || "Streaming hatoligi yuz berdi.");
        }
      );
    } catch (error) {
      globalStore.setSystemError("Kutilmagan tizim xatoligi: " + error.message);
    }
  }

  onStateUpdated(state) {
    const chatBox = this.shadowRoot.getElementById('chat-viewport');
    const btnSubmit = this.shadowRoot.getElementById('btn-submit-prompt');

    // Telemetry display updates
    this.shadowRoot.getElementById('m-tokens').textContent = state.metrics.totalTokensGenerated;
    this.shadowRoot.getElementById('m-context').textContent = `${state.messages.length} msgs`;

    btnSubmit.disabled = state.isStreaming;

    // Render Conversation Stack
    let htmlContent = '';
    state.messages.forEach((msg) => {
      htmlContent += `
        <div class="message-row ${msg.role}">
          <div class="msg-header">${msg.role.toUpperCase()} • ${msg.timestamp}</div>
          <div class="msg-bubble">${this.escapeHTML(msg.content)}</div>
        </div>
      `;
    });

    // Render Stream Chunk Buffer
    if (state.isStreaming) {
      htmlContent += `
        <div class="message-row assistant">
          <div class="msg-header">NEURA-CORE STREAMING...</div>
          <div class="msg-bubble streaming-cursor">${this.escapeHTML(state.currentStreamBuffer)}</div>
        </div>
      `;
    }

    chatBox.innerHTML = htmlContent;
    chatBox.scrollTop = chatBox.scrollHeight;
  }

  escapeHTML(str) {
    return str.replace(/[&<>'"]/g, 
      tag => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[tag] || tag)
    );
  }
}

// Custom Element Registration
customElements.define('ai-agent-interface', AIAgentInterface);

// ==========================================================================
// 6. SYSTEM INITIALIZATION & DIAGNOSTICS BINDINGS
// ==========================================================================

document.addEventListener('DOMContentLoaded', () => {
  // Init GPU Background Canvas
  window.neuraCanvas = new CyberBackgroundCanvas('cyber-background-canvas');

  // Modal Close Bindings
  const modal = document.getElementById('system-diagnostics-modal');
  const closeDiagBtn = document.getElementById('close-diag-btn');
  if (closeDiagBtn && modal) {
    closeDiagBtn.addEventListener('click', () => modal.close());
  }
});