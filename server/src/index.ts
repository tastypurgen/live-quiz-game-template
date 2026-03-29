import 'dotenv/config';

import { randomUUID } from 'node:crypto';
import { WebSocket, WebSocketServer } from 'ws';

import type {
  AnswerData,
  CreateGameData,
  Game,
  JoinGameData,
  Player,
  Question,
  RegData,
  StartGameData,
  User,
  WSMessage,
} from './types.js';

const PORT = process.env.PORT ? Number.parseInt(process.env.PORT, 10) : 3000;
const BASE_POINTS = 1000;
const ROOM_CODE_LENGTH = 6;
const QUESTION_RESULT_DELAY_MS = 3000;
const ROOM_CODE_ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';

const usersByName = new Map<string, User>();
const usersById = new Map<string, User>();
const gamesById = new Map<string, Game>();
const gamesByCode = new Map<string, Game>();
const socketUsers = new Map<WebSocket, string>();
const nextQuestionTimers = new Map<string, NodeJS.Timeout>();

const wss = new WebSocketServer({ port: PORT });

const sendMessage = (ws: WebSocket, type: string, data: unknown) => {
  if (ws.readyState !== WebSocket.OPEN) {
    return;
  }

  ws.send(
    JSON.stringify({
      type,
      data,
      id: 0,
    }),
  );
};

const sendError = (ws: WebSocket, message: string) => {
  sendMessage(ws, 'error', { message });
};

const sanitizePlayer = (player: Player) => ({
  name: player.name,
  index: player.index,
  score: player.score,
});

const getGameSockets = (game: Game) => {
  const sockets = new Set<WebSocket>();
  const host = usersById.get(game.hostId);

  if (host?.ws?.readyState === WebSocket.OPEN) {
    sockets.add(host.ws);
  }

  for (const player of game.players) {
    if (player.ws?.readyState === WebSocket.OPEN) {
      sockets.add(player.ws);
    }
  }

  return [...sockets];
};

const broadcastToGame = (game: Game, type: string, data: unknown) => {
  for (const ws of getGameSockets(game)) {
    sendMessage(ws, type, data);
  }
};

const broadcastPlayers = (game: Game) => {
  broadcastToGame(
    game,
    'update_players',
    game.players.map(sanitizePlayer),
  );
};

const generateRoomCode = () => {
  let code = '';

  do {
    code = Array.from({ length: ROOM_CODE_LENGTH }, () => {
      const index = Math.floor(Math.random() * ROOM_CODE_ALPHABET.length);
      return ROOM_CODE_ALPHABET[index];
    }).join('');
  } while (gamesByCode.has(code));

  return code;
};

const getRegisteredUser = (ws: WebSocket) => {
  const userId = socketUsers.get(ws);

  if (!userId) {
    return undefined;
  }

  return usersById.get(userId);
};

const findGameByHost = (userId: string) => {
  for (const game of gamesById.values()) {
    if (game.hostId === userId) {
      return game;
    }
  }

  return undefined;
};

const findGameByPlayer = (userId: string) => {
  for (const game of gamesById.values()) {
    if (game.players.some((player) => player.index === userId)) {
      return game;
    }
  }

  return undefined;
};

const findGameForUser = (userId: string) => findGameByHost(userId) ?? findGameByPlayer(userId);

const getPlayerFromGame = (game: Game, userId: string) =>
  game.players.find((player) => player.index === userId);

const clearQuestionTimer = (game: Game) => {
  if (game.questionTimer) {
    clearTimeout(game.questionTimer);
    game.questionTimer = undefined;
  }
};

const clearNextQuestionTimer = (gameId: string) => {
  const timer = nextQuestionTimers.get(gameId);

  if (timer) {
    clearTimeout(timer);
    nextQuestionTimers.delete(gameId);
  }
};

const cleanupGameTimers = (game: Game) => {
  clearQuestionTimer(game);
  clearNextQuestionTimer(game.id);
};

const buildScoreboard = (game: Game) => {
  const sortedPlayers = [...game.players].sort((left, right) => {
    if (right.score !== left.score) {
      return right.score - left.score;
    }

    return left.name.localeCompare(right.name);
  });

  let previousScore: number | undefined;
  let previousRank = 0;

  return sortedPlayers.map((player, index) => {
    const rank = previousScore === player.score ? previousRank : index + 1;

    previousScore = player.score;
    previousRank = rank;

    return {
      name: player.name,
      score: player.score,
      rank,
    };
  });
};

const finishGame = (game: Game) => {
  cleanupGameTimers(game);
  game.status = 'finished';

  broadcastToGame(game, 'game_finished', {
    scoreboard: buildScoreboard(game),
  });

  gamesById.delete(game.id);
  gamesByCode.delete(game.code);
};

const validateQuestion = (question: Question) => {
  if (typeof question.text !== 'string' || question.text.trim().length === 0) {
    return false;
  }

  if (!Array.isArray(question.options) || question.options.length !== 4) {
    return false;
  }

  if (question.options.some((option) => typeof option !== 'string' || option.trim().length === 0)) {
    return false;
  }

  if (!Number.isInteger(question.correctIndex) || question.correctIndex < 0 || question.correctIndex > 3) {
    return false;
  }

  if (!Number.isFinite(question.timeLimitSec) || question.timeLimitSec <= 0) {
    return false;
  }

  return true;
};

const sendQuestion = (game: Game, questionIndex: number) => {
  clearQuestionTimer(game);
  clearNextQuestionTimer(game.id);

  const question = game.questions[questionIndex];

  game.currentQuestion = questionIndex;
  game.questionStartTime = Date.now();
  game.playerAnswers.clear();

  game.questionTimer = setTimeout(() => {
    finalizeQuestion(game.id, questionIndex);
  }, question.timeLimitSec * 1000);

  broadcastToGame(game, 'question', {
    questionNumber: questionIndex + 1,
    totalQuestions: game.questions.length,
    text: question.text,
    options: question.options,
    timeLimitSec: question.timeLimitSec,
  });
};

const getPointsForAnswer = (game: Game, answerTimestamp: number) => {
  const question = game.questions[game.currentQuestion];

  if (!question || !game.questionStartTime) {
    return 0;
  }

  const totalMs = question.timeLimitSec * 1000;
  const elapsedMs = Math.max(0, answerTimestamp - game.questionStartTime);
  const remainingMs = Math.max(0, totalMs - elapsedMs);

  return Math.round(BASE_POINTS * (remainingMs / totalMs));
};

const finalizeQuestion = (gameId: string, questionIndex: number) => {
  const game = gamesById.get(gameId);

  if (!game || game.status !== 'in_progress' || game.currentQuestion !== questionIndex) {
    return;
  }

  const question = game.questions[questionIndex];

  if (!question) {
    finishGame(game);
    return;
  }

  clearQuestionTimer(game);

  const playerResults = game.players.map((player) => {
    const answer = game.playerAnswers.get(player.index);
    const answered = Boolean(answer);
    const correct = answer?.answerIndex === question.correctIndex;
    const pointsEarned = correct && answer ? getPointsForAnswer(game, answer.timestamp) : 0;

    if (pointsEarned > 0) {
      player.score += pointsEarned;
    }

    return {
      name: player.name,
      answered,
      correct,
      pointsEarned,
      totalScore: player.score,
    };
  });

  broadcastToGame(game, 'question_result', {
    questionIndex,
    correctIndex: question.correctIndex,
    playerResults,
  });

  const nextQuestionIndex = questionIndex + 1;

  if (nextQuestionIndex >= game.questions.length) {
    finishGame(game);
    return;
  }

  const timer = setTimeout(() => {
    nextQuestionTimers.delete(game.id);
    sendQuestion(game, nextQuestionIndex);
  }, QUESTION_RESULT_DELAY_MS);

  nextQuestionTimers.set(game.id, timer);
};

const removePlayerFromGame = (game: Game, userId: string) => {
  const playerIndex = game.players.findIndex((player) => player.index === userId);

  if (playerIndex === -1) {
    return;
  }

  game.players.splice(playerIndex, 1);
  game.playerAnswers.delete(userId);

  broadcastPlayers(game);

  if (game.status === 'in_progress') {
    if (game.players.length === 0) {
      finishGame(game);
      return;
    }

    if (game.playerAnswers.size >= game.players.length) {
      finalizeQuestion(game.id, game.currentQuestion);
    }
  }
};

const handleRegistration = (ws: WebSocket, data: unknown) => {
  const payload = data as RegData;

  if (
    !payload ||
    typeof payload.name !== 'string' ||
    typeof payload.password !== 'string' ||
    payload.name.trim().length === 0 ||
    payload.password.trim().length === 0
  ) {
    sendMessage(ws, 'reg', {
      name: '',
      index: '',
      error: true,
      errorText: 'Name and password are required.',
    });
    return;
  }

  const name = payload.name.trim();
  const password = payload.password.trim();
  let user = usersByName.get(name);

  if (user && user.password !== password) {
    sendMessage(ws, 'reg', {
      name,
      index: '',
      error: true,
      errorText: 'Invalid password.',
    });
    return;
  }

  if (!user) {
    user = {
      name,
      password,
      index: randomUUID(),
      ws,
    };

    usersByName.set(name, user);
    usersById.set(user.index, user);
  } else if (user.ws && user.ws !== ws) {
    user.ws.close(1000, 'Session replaced by a new connection.');
  }

  user.ws = ws;
  socketUsers.set(ws, user.index);

  const activePlayerGame = findGameByPlayer(user.index);
  const activePlayer = activePlayerGame ? getPlayerFromGame(activePlayerGame, user.index) : undefined;

  if (activePlayer) {
    activePlayer.ws = ws;
  }

  sendMessage(ws, 'reg', {
    name: user.name,
    index: user.index,
    error: false,
    errorText: '',
  });
};

const handleCreateGame = (ws: WebSocket, data: unknown) => {
  const user = getRegisteredUser(ws);

  if (!user) {
    sendError(ws, 'You must register before creating a game.');
    return;
  }

  const existingGame = findGameForUser(user.index);

  if (existingGame && existingGame.status !== 'finished') {
    sendError(ws, 'You are already participating in another game.');
    return;
  }

  const payload = data as CreateGameData;

  if (!payload || !Array.isArray(payload.questions) || payload.questions.length === 0) {
    sendError(ws, 'At least one valid question is required.');
    return;
  }

  if (!payload.questions.every(validateQuestion)) {
    sendError(ws, 'Each question must have 4 options, a valid correct answer, and a positive time limit.');
    return;
  }

  const game: Game = {
    id: randomUUID(),
    code: generateRoomCode(),
    hostId: user.index,
    questions: payload.questions,
    players: [],
    currentQuestion: -1,
    status: 'waiting',
    playerAnswers: new Map(),
  };

  gamesById.set(game.id, game);
  gamesByCode.set(game.code, game);

  sendMessage(ws, 'game_created', {
    gameId: game.id,
    code: game.code,
  });
};

const handleJoinGame = (ws: WebSocket, data: unknown) => {
  const user = getRegisteredUser(ws);

  if (!user) {
    sendError(ws, 'You must register before joining a game.');
    return;
  }

  const existingGame = findGameForUser(user.index);

  if (existingGame && existingGame.status !== 'finished') {
    sendError(ws, 'You are already participating in another game.');
    return;
  }

  const payload = data as JoinGameData;
  const code = payload?.code?.trim().toUpperCase();

  if (!code) {
    sendError(ws, 'Room code is required.');
    return;
  }

  const game = gamesByCode.get(code);

  if (!game) {
    sendError(ws, 'Game not found.');
    return;
  }

  if (game.status !== 'waiting') {
    sendError(ws, 'This game has already started.');
    return;
  }

  if (game.hostId === user.index) {
    sendError(ws, 'Host cannot join their own game as a player.');
    return;
  }

  const player: Player = {
    name: user.name,
    index: user.index,
    score: 0,
    ws,
  };

  game.players.push(player);

  sendMessage(ws, 'game_joined', {
    gameId: game.id,
  });

  broadcastToGame(game, 'player_joined', {
    playerName: player.name,
    playerCount: game.players.length,
  });
  broadcastPlayers(game);
};

const handleStartGame = (ws: WebSocket, data: unknown) => {
  const user = getRegisteredUser(ws);

  if (!user) {
    sendError(ws, 'You must register before starting a game.');
    return;
  }

  const payload = data as StartGameData;
  const game = payload?.gameId ? gamesById.get(payload.gameId) : undefined;

  if (!game) {
    sendError(ws, 'Game not found.');
    return;
  }

  if (game.hostId !== user.index) {
    sendError(ws, 'Only the host can start the game.');
    return;
  }

  if (game.status !== 'waiting') {
    sendError(ws, 'Game has already started.');
    return;
  }

  if (game.players.length === 0) {
    sendError(ws, 'At least one player must join before starting.');
    return;
  }

  game.status = 'in_progress';
  sendQuestion(game, 0);
};

const handleAnswer = (ws: WebSocket, data: unknown) => {
  const user = getRegisteredUser(ws);

  if (!user) {
    sendError(ws, 'You must register before answering.');
    return;
  }

  const payload = data as AnswerData;
  const game = payload?.gameId ? gamesById.get(payload.gameId) : undefined;

  if (!game) {
    sendError(ws, 'Game not found.');
    return;
  }

  if (game.status !== 'in_progress') {
    sendError(ws, 'Game is not currently in progress.');
    return;
  }

  const player = getPlayerFromGame(game, user.index);

  if (!player) {
    sendError(ws, 'Only joined players can answer questions.');
    return;
  }

  if (!Number.isInteger(payload.questionIndex) || payload.questionIndex !== game.currentQuestion) {
    sendError(ws, 'Answer does not match the current question.');
    return;
  }

  if (!Number.isInteger(payload.answerIndex) || payload.answerIndex < 0 || payload.answerIndex > 3) {
    sendError(ws, 'Answer index must be between 0 and 3.');
    return;
  }

  if (game.playerAnswers.has(user.index)) {
    sendError(ws, 'You have already answered this question.');
    return;
  }

  const question = game.questions[game.currentQuestion];

  if (!question || !game.questionStartTime) {
    sendError(ws, 'No active question.');
    return;
  }

  const now = Date.now();
  const elapsedMs = now - game.questionStartTime;

  if (elapsedMs >= question.timeLimitSec * 1000) {
    sendError(ws, 'Time is up for this question.');
    return;
  }

  game.playerAnswers.set(user.index, {
    answerIndex: payload.answerIndex,
    timestamp: now,
  });

  sendMessage(ws, 'answer_accepted', {
    questionIndex: payload.questionIndex,
  });

  if (game.playerAnswers.size >= game.players.length) {
    finalizeQuestion(game.id, game.currentQuestion);
  }
};

const handleMessage = (ws: WebSocket, rawMessage: Buffer) => {
  let message: WSMessage;

  try {
    message = JSON.parse(rawMessage.toString()) as WSMessage;
  } catch {
    sendError(ws, 'Invalid JSON payload.');
    return;
  }

  if (!message || typeof message.type !== 'string') {
    sendError(ws, 'Invalid message format.');
    return;
  }

  switch (message.type) {
    case 'reg':
      handleRegistration(ws, message.data);
      return;
    case 'create_game':
      handleCreateGame(ws, message.data);
      return;
    case 'join_game':
      handleJoinGame(ws, message.data);
      return;
    case 'start_game':
      handleStartGame(ws, message.data);
      return;
    case 'answer':
      handleAnswer(ws, message.data);
      return;
    default:
      sendError(ws, `Unsupported message type: ${message.type}`);
  }
};

const handleDisconnect = (ws: WebSocket) => {
  const userId = socketUsers.get(ws);

  socketUsers.delete(ws);

  if (!userId) {
    return;
  }

  const user = usersById.get(userId);

  if (!user || user.ws !== ws) {
    return;
  }

  user.ws = undefined;

  const playerGame = findGameByPlayer(userId);

  if (playerGame) {
    removePlayerFromGame(playerGame, userId);
  }
};

wss.on('connection', (ws) => {
  ws.on('message', (message) => {
    handleMessage(ws, Buffer.isBuffer(message) ? message : Buffer.from(message.toString()));
  });

  ws.on('close', () => {
    handleDisconnect(ws);
  });
});

console.log(`WebSocket server running at ws://localhost:${PORT}`);
